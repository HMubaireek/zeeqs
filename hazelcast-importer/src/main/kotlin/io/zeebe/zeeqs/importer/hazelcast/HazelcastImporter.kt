/*
 * Enhanced HazelcastImporter with:
 * - Partitioned multithreading for safe concurrent record handling
 * - Hybrid time+size batching for database writes
 * - Immediate vs. deferred update publishing
 */

 package io.zeebe.zeeqs.importer.hazelcast

 import com.hazelcast.client.HazelcastClient
 import com.hazelcast.client.config.ClientConfig
 import io.camunda.zeebe.protocol.Protocol
 import io.zeebe.exporter.proto.Schema
 import io.zeebe.exporter.proto.Schema.RecordMetadata.RecordType
 import io.zeebe.hazelcast.connect.java.ZeebeHazelcast
 import io.zeebe.zeeqs.data.entity.*
 import io.zeebe.zeeqs.data.reactive.DataUpdatesPublisher
 import io.zeebe.zeeqs.data.repository.*
 import io.zeebe.zeeqs.importer.hazelcast.ProtobufTransformer.structToMap
 import org.springframework.stereotype.Component
 import java.time.Duration
 import java.util.Collections
 import java.util.concurrent.ConcurrentHashMap
 import java.util.concurrent.ExecutorService
 import java.util.concurrent.Executors
 import java.util.concurrent.ScheduledExecutorService
 import java.util.concurrent.TimeUnit
 import javax.annotation.PreDestroy

@Component
 class HazelcastImporter(
     val hazelcastConfigRepository: HazelcastConfigRepository,
     val processRepository: ProcessRepository,
     val processInstanceRepository: ProcessInstanceRepository,
     val elementInstanceRepository: ElementInstanceRepository,
     val elementInstanceStateTransitionRepository: ElementInstanceStateTransitionRepository,
     val variableRepository: VariableRepository,
     val variableUpdateRepository: VariableUpdateRepository,
     val jobRepository: JobRepository,
     val userTaskRepository: UserTaskRepository,
     val incidentRepository: IncidentRepository,
     val timerRepository: TimerRepository,
     val messageRepository: MessageRepository,
     val messageVariableRepository: MessageVariableRepository,
     val messageSubscriptionRepository: MessageSubscriptionRepository,
     val messageCorrelationRepository: MessageCorrelationRepository,
     val errorRepository: ErrorRepository,
     private val decisionEvaluationImporter: HazelcastDecisionImporter,
     private val signalImporter: HazelcastSignalImporter,
     private val dataUpdatesPublisher: DataUpdatesPublisher // Renamed from `dataUpdatesPublisher`
 ) {
 
     // --- Multithreading Configuration ---
     private val NUM_PARTITIONS = 8 // Increased partition count, can be tuned
     private val executors: Array<ExecutorService> = Array(NUM_PARTITIONS) { Executors.newSingleThreadExecutor() }
     private val processingTasks = ConcurrentHashMap<Long, Runnable>() // Simple map to track active tasks for a key (optional, for debugging/monitoring)
 
     // --- Batching Configuration ---
     private val BATCH_SIZE = 200 // Increased batch size, can be tuned
     private val BATCH_DELAY_MS = 1000L // Increased delay, can be tuned
 
     private val variableBatcher = Batcher<Variable>(BATCH_SIZE, BATCH_DELAY_MS) { variableRepository.saveAll(it) }
     private val variableUpdateBatcher = Batcher<VariableUpdate>(BATCH_SIZE, BATCH_DELAY_MS) { variableUpdateRepository.saveAll(it) }
     private val elementInstanceStateTransitionBatcher = Batcher<ElementInstanceStateTransition>(BATCH_SIZE, BATCH_DELAY_MS) { elementInstanceStateTransitionRepository.saveAll(it) }
     // Add batchers for other entities if they become high volume and less critical for immediate UI
     // private val elementInstanceBatcher = Batcher<ElementInstance>(BATCH_SIZE, BATCH_DELAY_MS) { elementInstanceRepository.saveAll(it) }
 
     var zeebeHazelcast: ZeebeHazelcast? = null
 
     fun start(hazelcastProperties: HazelcastProperties) {
 
         val hazelcastConnection = hazelcastProperties.connection
         val hazelcastConnectionTimeout = Duration.parse(hazelcastProperties.connectionTimeout)
         val hazelcastRingbuffer = hazelcastProperties.ringbuffer
         val hazelcastConnectionInitialBackoff =
             Duration.parse(hazelcastProperties.connectionInitialBackoff)
         val hazelcastConnectionBackoffMultiplier = hazelcastProperties.connectionBackoffMultiplier
         val hazelcastConnectionMaxBackoff = Duration.parse(hazelcastProperties.connectionMaxBackoff)
 
         val hazelcastConfig = hazelcastConfigRepository.findById(hazelcastConnection)
             .orElse(
                 HazelcastConfig(
                     id = hazelcastConnection,
                     sequence = -1
                 )
             )
 
         // This post-process listener runs in the Hazelcast reader thread *after* reading,
         // but *before* our submitted task is necessarily completed.
         // This is acceptable as we prioritize reading speed and assume the worker threads
         // and batchers will eventually persist the data. A more robust solution might
         // track completed sequence numbers across all workers.
         val updateSequence: ((Long) -> Unit) = {
             hazelcastConfig.sequence = it
             hazelcastConfigRepository.save(hazelcastConfig)
             // Note: Saving the sequence is not batched, happens immediately
         }
 
         val clientConfig = ClientConfig()
         val networkConfig = clientConfig.networkConfig
         networkConfig.addresses = listOf(hazelcastConnection)
 
         val connectionRetryConfig = clientConfig.connectionStrategyConfig.connectionRetryConfig
         connectionRetryConfig.clusterConnectTimeoutMillis = hazelcastConnectionTimeout.toMillis()
         connectionRetryConfig.initialBackoffMillis =
             hazelcastConnectionInitialBackoff.toMillis().toInt()
         connectionRetryConfig.multiplier = hazelcastConnectionBackoffMultiplier
         connectionRetryConfig.maxBackoffMillis = hazelcastConnectionMaxBackoff.toMillis().toInt()
 
         val hazelcast = HazelcastClient.newHazelcastClient(clientConfig)
 
         val builder = ZeebeHazelcast.newBuilder(hazelcast).name(hazelcastRingbuffer)
             // Partition by Process Definition Key
             .addProcessListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.processDefinitionKey) { this.importProcess(it) }
                  }
             }
             // Partition by Process Instance Key for Process Instance entity updates,
             // and by Element Instance Key (metadata.key) for Element Instance updates.
             .addProcessInstanceListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                     // Task for Process Instance entity (if root PI)
                     if (it.processInstanceKey == it.metadata.key) {
                        submitToPartition(it.processInstanceKey) { this.importProcessInstance(it) }
                     }
                     // Task for Element Instance entity
                     submitToPartition(it.metadata.key) { this.importElementInstance(it) }
                     // Task for Element Instance State Transition entity (batched)
                     submitToPartition(it.metadata.key) { this.importElementInstanceStateTransition(it) }
                 }
             }
             // Partition by Variable Key
             .addVariableListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                     submitToPartition(it.metadata.key) { this.importVariableRecord(it) }
                 }
             }
             // Partition by Job Key
             .addJobListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                     submitToPartition(it.metadata.key) { this.importJobRecord(it) }
                 }
             }
              // Partition by Incident Key
             .addIncidentListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.metadata.key) { this.importIncidentRecord(it) }
                 }
             }
              // Partition by Timer Key
             .addTimerListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.metadata.key) { this.importTimerRecord(it) }
                  }
             }
              // Partition by Message Key
             .addMessageListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.metadata.key) { this.importMessageRecord(it) }
                  }
             }
              // Partition by Message Start Event Subscription Key
             .addMessageStartEventSubscriptionListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.metadata.key) { this.importMessageStartEventSubscriptionRecord(it) }
                  }
             }
              // Partition by Process Message Subscription Key
             .addProcessMessageSubscriptionListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      submitToPartition(it.metadata.key) { this.importProcessMessageSubscriptionRecord(it) }
                  }
             }
             // Delegate to Decision Importer - assumes Decision Importer handles threading/batching internally or is single-threaded
             .addDecisionListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      // If DecisionImporter is not thread-safe, wrap this in submitToPartition
                      // submitToPartition(it.metadata.key) { decisionEvaluationImporter.importDecision(it) }
                      decisionEvaluationImporter.importDecision(it)
                  }
             }
             // Delegate to Decision Importer
             .addDecisionRequirementsListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      // If DecisionImporter is not thread-safe, wrap this in submitToPartition
                      // submitToPartition(it.metadata.key) { decisionEvaluationImporter.importDecisionRequirements(it) }
                     decisionEvaluationImporter.importDecisionRequirements(it)
                 }
             }
             // Delegate to Decision Importer
             .addDecisionEvaluationListener { record ->
                 record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                     // If DecisionImporter is not thread-safe, wrap this in submitToPartition
                     // submitToPartition(it.metadata.key) { decisionEvaluationImporter.importDecisionEvaluation(it) }
                     decisionEvaluationImporter.importDecisionEvaluation(it)
                 }
             }
             // Delegate to Signal Importer - assumes Signal Importer handles threading/batching internally or is single-threaded
             .addSignalListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      // If SignalImporter is not thread-safe, wrap this in submitToPartition
                      // submitToPartition(it.metadata.key) { signalImporter.importSignal(it) }
                      signalImporter.importSignal(it)
                  }
             }
              // Delegate to Signal Importer
             .addSignalSubscriptionListener { record ->
                  record.takeIf { it.metadata.recordType == RecordType.EVENT }?.let {
                      // If SignalImporter is not thread-safe, wrap this in submitToPartition
                      // submitToPartition(it.metadata.key) { signalImporter.importSignalSubscription(it) }
                     signalImporter.importSignalSubscription(it)
                 }
             }
             // Error records might not have a key, partition randomly or use processInstanceKey
             .addErrorListener { record ->
                 // Use processInstanceKey for partitioning if available, otherwise partition randomly
                 val partitionKey = record.processInstanceKey.takeIf { it > 0 } ?: record.metadata.position // position as a fallback key
                 submitToPartition(partitionKey) { this.importError(record) }
             }
             .postProcessListener(updateSequence)
 
         if (hazelcastConfig.sequence >= 0) {
             builder.readFrom(hazelcastConfig.sequence)
         } else {
             // Start reading from the beginning if no sequence is stored
             builder.readFromHead()
         }
 
         zeebeHazelcast = builder.build()
     }
 
     @PreDestroy // Ensure stop is called on Spring context shutdown
     fun stop() {
         println("Shutting down Hazelcast Importer...")
         zeebeHazelcast?.close() // Stop consuming from Hazelcast
 
         println("Flushing pending batches...")
         // Flush all batchers to save any remaining items
         variableBatcher.stop()
         variableUpdateBatcher.stop()
         elementInstanceStateTransitionBatcher.stop()
         // Add stop calls for any other batchers
 
         println("Shutting down executor service...")
         // Shutdown executors and wait for tasks to complete
         executors.forEach { executor ->
             executor.shutdown()
             try {
                 // Wait a reasonable time for tasks to finish
                 if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                     println("Executor did not terminate in time, forcing shutdown.")
                     executor.shutdownNow()
                 }
             } catch (e: InterruptedException) {
                 println("Executor shutdown interrupted.")
                 executor.shutdownNow()
                 Thread.currentThread().interrupt()
             }
         }
         println("Hazelcast Importer stopped.")
     }
 
     // Helper function to submit a task to a specific partition executor
     private fun submitToPartition(key: Long, task: () -> Unit) {
         // Use Math.abs to handle potential negative keys, then modulo for partition index
         val partition = Math.abs(key.toInt() % NUM_PARTITIONS)
         executors[partition].submit(task)
     }
 
     // --- Original Import Logic - Modified to fit multithreading/batching/publisher strategy ---
 
     private fun getPartitionIdWithPosition(metadata: Schema.RecordMetadata) =
         "${metadata.partitionId}-${metadata.position}"
 
     private fun importProcess(process: Schema.ProcessRecord) {
         // Process is critical for UI, save and publish immediately (within the process's partition thread)
         val entity = processRepository
             .findById(process.processDefinitionKey)
             .orElse(createProcess(process))
 
         processRepository.save(entity)
         dataUpdatesPublisher.onProcessUpdated(entity)
     }
 
     private fun createProcess(process: Schema.ProcessRecord): Process {
         return Process(
             key = process.processDefinitionKey,
             bpmnProcessId = process.bpmnProcessId,
             version = process.version,
             bpmnXML = process.resource.toStringUtf8(),
             deployTime = process.metadata.timestamp,
             resourceName = process.resourceName,
             checksum = process.checksum.toStringUtf8()
         )
     }
 
     // This method is now split in the listener:
     // - importProcessInstance for the root PI record (partitioned by processInstanceKey)
     // - importElementInstance for the element instance part (partitioned by metadata.key)
     // - importElementInstanceStateTransition for the state transition part (partitioned by metadata.key, batched)
     // The original combined method `importProcessInstanceRecord` is no longer called directly from the listener
     // but its logic is distributed.
 
     private fun importProcessInstance(record: Schema.ProcessInstanceRecord) {
         // Process Instance is critical for UI, save and publish immediately (within the PI's partition thread)
         // This method is only called from the listener when record.processInstanceKey == record.metadata.key
         val entity = processInstanceRepository
             .findById(record.processInstanceKey)
             .orElse(createProcessInstance(record))
 
         when (record.metadata.intent) {
             "ELEMENT_ACTIVATED" -> { // This intent applies to the root PI as well
                 entity.startTime = record.metadata.timestamp
                 entity.state = ProcessInstanceState.ACTIVATED
             }
 
             "ELEMENT_COMPLETED" -> {
                 entity.endTime = record.metadata.timestamp
                 entity.state = ProcessInstanceState.COMPLETED
             }
 
             "ELEMENT_TERMINATED" -> {
                 entity.endTime = record.metadata.timestamp
                 entity.state = ProcessInstanceState.TERMINATED
             }
         }
 
         processInstanceRepository.save(entity)
         dataUpdatesPublisher.onProcessInstanceUpdated(entity)
     }
 
     private fun createProcessInstance(record: Schema.ProcessInstanceRecord): ProcessInstance {
         return ProcessInstance(
             key = record.processInstanceKey,
             position = record.metadata.position, // Note: position here might not be the PI root position if record.processInstanceKey != record.metadata.key, but this method is only called when they are equal.
             bpmnProcessId = record.bpmnProcessId,
             version = record.version,
             processDefinitionKey = record.processDefinitionKey,
             parentProcessInstanceKey = record.parentProcessInstanceKey.takeIf { it > 0 },
             parentElementInstanceKey = record.parentElementInstanceKey.takeIf { it > 0 }
         )
     }
 
     private fun importElementInstance(record: Schema.ProcessInstanceRecord) {
         // Element Instance state changes are critical for UI, save and publish immediately (within the element instance's partition thread)
         val entity = elementInstanceRepository
             .findById(record.metadata.key)
             .orElse(createElementInstance(record))
 
         entity.state = getElementInstanceState(record)
 
         when (record.metadata.intent) {
             "ELEMENT_ACTIVATING" -> {
                 entity.startTime = record.metadata.timestamp
             }
 
             "ELEMENT_COMPLETED", "ELEMENT_TERMINATED" -> {
                 entity.endTime = record.metadata.timestamp
             }
 
             "SEQUENCE_FLOW_TAKEN" -> {
                 entity.startTime = record.metadata.timestamp // Sequence flows have instant start/end
                 entity.endTime = record.metadata.timestamp
             }
         }
 
         elementInstanceRepository.save(entity)
         dataUpdatesPublisher.onElementInstanceUpdated(entity) // Publish UI update
     }
 
     private fun createElementInstance(record: Schema.ProcessInstanceRecord): ElementInstance {
         // Mapping logic remains the same
         val bpmnElementType = when (record.bpmnElementType) {
             "UNSPECIFIED" -> BpmnElementType.UNSPECIFIED
             "BOUNDARY_EVENT" -> BpmnElementType.BOUNDARY_EVENT
             "CALL_ACTIVITY" -> BpmnElementType.CALL_ACTIVITY
             "END_EVENT" -> BpmnElementType.END_EVENT
             "EVENT_BASED_GATEWAY" -> BpmnElementType.EVENT_BASED_GATEWAY
             "EXCLUSIVE_GATEWAY" -> BpmnElementType.EXCLUSIVE_GATEWAY
             "INTERMEDIATE_CATCH_EVENT" -> BpmnElementType.INTERMEDIATE_CATCH_EVENT
             "INTERMEDIATE_THROW_EVENT" -> BpmnElementType.INTERMEDIATE_THROW_EVENT
             "PARALLEL_GATEWAY" -> BpmnElementType.PARALLEL_GATEWAY
             "PROCESS" -> BpmnElementType.PROCESS // Note: The root Process Instance record has type PROCESS
             "RECEIVE_TASK" -> BpmnElementType.RECEIVE_TASK
             "SEQUENCE_FLOW" -> BpmnElementType.SEQUENCE_FLOW
             "SERVICE_TASK" -> BpmnElementType.SERVICE_TASK
             "START_EVENT" -> BpmnElementType.START_EVENT
             "SUB_PROCESS" -> BpmnElementType.SUB_PROCESS
             "EVENT_SUB_PROCESS" -> BpmnElementType.EVENT_SUB_PROCESS
             "MULTI_INSTANCE_BODY" -> BpmnElementType.MULTI_INSTANCE_BODY
             "USER_TASK" -> BpmnElementType.USER_TASK
             "MANUAL_TASK" -> BpmnElementType.MANUAL_TASK
             "BUSINESS_RULE_TASK" -> BpmnElementType.BUSINESS_RULE_TASK
             "SCRIPT_TASK" -> BpmnElementType.SCRIPT_TASK
             "SEND_TASK" -> BpmnElementType.SEND_TASK
             "INCLUSIVE_GATEWAY" -> BpmnElementType.INCLUSIVE_GATEWAY
             else -> BpmnElementType.UNKNOWN
         }
 
         return ElementInstance(
             key = record.metadata.key,
             position = record.metadata.position,
             elementId = record.elementId,
             bpmnElementType = bpmnElementType,
             processInstanceKey = record.processInstanceKey,
             processDefinitionKey = record.processDefinitionKey,
             scopeKey = record.flowScopeKey.takeIf { it > 0 }
         )
     }
 
     private fun getElementInstanceState(record: Schema.ProcessInstanceRecord): ElementInstanceState {
         // State mapping remains the same
         return when (record.metadata.intent) {
             "ELEMENT_ACTIVATING" -> ElementInstanceState.ACTIVATING
             "ELEMENT_ACTIVATED" -> ElementInstanceState.ACTIVATED
             "ELEMENT_COMPLETING" -> ElementInstanceState.COMPLETING
             "ELEMENT_COMPLETED" -> ElementInstanceState.COMPLETED
             "ELEMENT_TERMINATING" -> ElementInstanceState.TERMINATING
             "ELEMENT_TERMINATED" -> ElementInstanceState.TERMINATED
             "SEQUENCE_FLOW_TAKEN" -> ElementInstanceState.TAKEN
             // Use ACTIVATING as a reasonable default if an unexpected intent is encountered,
             // though ideally all relevant intents are mapped.
             else -> ElementInstanceState.ACTIVATING
         }
     }
 
     private fun importElementInstanceStateTransition(record: Schema.ProcessInstanceRecord) {
         // State transitions can be high volume, add to batcher (within the element instance's partition thread)
         val state = getElementInstanceState(record)
         val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
 
         // Note: This finds or creates in the thread, then adds to batcher buffer.
         // The actual save happens later in the batcher's thread.
         val entity = elementInstanceStateTransitionRepository
             .findById(partitionIdWithPosition)
             .orElse(
                 ElementInstanceStateTransition(
                     partitionIdWithPosition = partitionIdWithPosition,
                     elementInstanceKey = record.metadata.key,
                     timestamp = record.metadata.timestamp,
                     state = state
                 )
             )
 
         elementInstanceStateTransitionBatcher.add(entity)
         // Do NOT publish UI update for every state transition immediately
     }
 
     private fun importVariableRecord(record: Schema.VariableRecord) {
         // Variables and Variable Updates can be high volume.
         // Add Variable entity to batcher.
         val variable = variableRepository
             .findById(record.metadata.key)
             .orElse(createVariable(record)).apply {
                 value = record.value
                 timestamp = record.metadata.timestamp
             }
         variableBatcher.add(variable) // Defer Variable entity write
 
         // Add Variable Update entity to batcher.
         val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
         val variableUpdate = variableUpdateRepository
             .findById(partitionIdWithPosition)
             .orElse(
                 VariableUpdate(
                     partitionIdWithPosition = partitionIdWithPosition,
                     variableKey = record.metadata.key,
                     name = record.name,
                     value = record.value,
                     processInstanceKey = record.processInstanceKey,
                     scopeKey = record.scopeKey,
                     timestamp = record.metadata.timestamp
                 )
             )
         variableUpdateBatcher.add(variableUpdate) // Defer VariableUpdate entity write
 
         // Do NOT publish UI update for every variable change immediately
     }
 
     private fun createVariable(record: Schema.VariableRecord): Variable {
         return Variable(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Variable record position
             name = record.name,
             value = record.value,
             processInstanceKey = record.processInstanceKey,
             processDefinitionKey = record.processDefinitionKey,
             scopeKey = record.scopeKey,
             timestamp = record.metadata.timestamp
         )
     }
 
     // VariableUpdate creation is inlined in importVariableRecord now
 
     private fun importJobRecord(record: Schema.JobRecord) {
         // Job lifecycle events are critical for UI.
         // User Tasks are a specific job type, handle them within the job's partition thread.
         if (isJobForUserTask(record)) {
             importUserTask(record) // This will save UserTask and publish
         } else {
             importJobForWorker(record) // This will save Job and publish
         }
     }
 
     private fun isJobForUserTask(record: Schema.JobRecord) =
         record.type == Protocol.USER_TASK_JOB_TYPE
 
     private fun importJobForWorker(record: Schema.JobRecord) {
         // Job entity is critical for UI, save and publish immediately (within the job's partition thread)
         val entity = jobRepository
             .findById(record.metadata.key)
             .orElse(createJob(record))
 
         when (record.metadata.intent) {
             "CREATED" -> {
                 entity.state = JobState.ACTIVATABLE
                 entity.startTime = record.metadata.timestamp
             }
 
             "TIMED_OUT", "RETRIES_UPDATED" -> entity.state = JobState.ACTIVATABLE
             "FAILED" -> entity.state = JobState.FAILED
             "COMPLETED" -> {
                 entity.state = JobState.COMPLETED
                 entity.endTime = record.metadata.timestamp
             }
 
             "CANCELED" -> {
                 entity.state = JobState.CANCELED
                 entity.endTime = record.metadata.timestamp
             }
 
             "ERROR_THROWN" -> {
                 entity.state = JobState.ERROR_THROWN
                 entity.endTime = record.metadata.timestamp
             }
         }
 
         entity.worker = record.worker.ifEmpty { null }
         entity.retries = record.retries
         entity.timestamp = record.metadata.timestamp // Update timestamp on each state change
 
         jobRepository.save(entity)
         dataUpdatesPublisher.onJobUpdated(entity) // Publish UI update
     }
 
     private fun createJob(record: Schema.JobRecord): Job {
         return Job(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Job record position
             jobType = record.type,
             processInstanceKey = record.processInstanceKey,
             elementInstanceKey = record.elementInstanceKey,
             processDefinitionKey = record.processDefinitionKey
         )
     }
 
     private fun importUserTask(record: Schema.JobRecord) {
         // User Task entity is critical for UI, save and publish immediately (within the job's partition thread)
         // Note: User Task is a Job entity with specific headers and state mapping.
         val entity = userTaskRepository
             .findById(record.metadata.key)
             .orElse(createUserTask(record))
 
         when (record.metadata.intent) {
             "CREATED" -> {
                 // User Task is created when the corresponding Job is created
                 entity.startTime = record.metadata.timestamp
                 entity.state = UserTaskState.CREATED // Add CREATED state
             }
 
             "COMPLETED" -> {
                 entity.state = UserTaskState.COMPLETED
                 entity.endTime = record.metadata.timestamp
             }
 
             "CANCELED" -> {
                 entity.state = UserTaskState.CANCELED
                 entity.endTime = record.metadata.timestamp
             }
             // Other Job intents like ACTIVATABLE, FAILED, ERROR_THROWN might also apply to the underlying Job,
             // but the UserTask entity might only track its specific lifecycle.
             // Add state mapping for other relevant job intents if needed for UserTask entity.
             // E.g., "ACTIVATED" -> entity.state = UserTaskState.ASSIGNED/CLAIMED? (Depends on UI/Entity design)
         }
 
         entity.timestamp = record.metadata.timestamp
 
         userTaskRepository.save(entity)
         // Consider adding a dedicated onUserTaskUpdated publisher event if the UI needs it separately
         // dataUpdatesPublisher.onUserTaskUpdated(entity)
         // Or just rely on the Job update publisher if UserTasks are displayed as Jobs
          dataUpdatesPublisher.onJobUpdated(jobRepository.findById(record.metadata.key).orElseThrow()) // Publish the underlying job update
     }
 
     private fun createUserTask(record: Schema.JobRecord): UserTask {
         // User Task entity creation relies on Job headers
         val customHeaders = record.customHeaders.fieldsMap
         val assignee = customHeaders[Protocol.USER_TASK_ASSIGNEE_HEADER_NAME]?.stringValue
         val candidateGroups =
             customHeaders[Protocol.USER_TASK_CANDIDATE_GROUPS_HEADER_NAME]?.stringValue
         val formKey = customHeaders[Protocol.USER_TASK_FORM_KEY_HEADER_NAME]?.stringValue
         return UserTask(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Job record position
             processInstanceKey = record.processInstanceKey,
             processDefinitionKey = record.processDefinitionKey,
             elementInstanceKey = record.elementInstanceKey,
             assignee = assignee,
             candidateGroups = candidateGroups,
             formKey = formKey
         )
     }
 
     private fun importIncidentRecord(record: Schema.IncidentRecord) {
         // Incident is critical for UI, save and publish immediately (within the incident's partition thread)
         val entity = incidentRepository
             .findById(record.metadata.key)
             .orElse(createIncident(record))
 
         when (record.metadata.intent) {
             "CREATED" -> {
                 entity.state = IncidentState.CREATED
                 entity.creationTime = record.metadata.timestamp
             }
 
             "RESOLVED" -> {
                 entity.state = IncidentState.RESOLVED
                 entity.resolveTime = record.metadata.timestamp
             }
         }
 
         incidentRepository.save(entity)
         dataUpdatesPublisher.onIncidentUpdated(entity) // Publish UI update
     }
 
     private fun createIncident(record: Schema.IncidentRecord): Incident {
         return Incident(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Incident record position
             errorType = record.errorType,
             errorMessage = record.errorMessage,
             processInstanceKey = record.processInstanceKey,
             processDefinitionKey = record.processDefinitionKey,
             elementInstanceKey = record.elementInstanceKey,
             jobKey = record.jobKey.takeIf { it > 0 }
         )
     }
 
     private fun importTimerRecord(record: Schema.TimerRecord) {
         // Timer lifecycle events can be critical for UI. Save and publish immediately (within the timer's partition thread)
         val entity = timerRepository
             .findById(record.metadata.key)
             .orElse(createTimer(record))
 
         when (record.metadata.intent) {
             "CREATED" -> {
                 entity.state = TimerState.CREATED
                 entity.startTime = record.metadata.timestamp
             }
 
             "TRIGGERED" -> {
                 entity.state = TimerState.TRIGGERED
                 entity.endTime = record.metadata.timestamp
                 entity.processInstanceKey = record.processInstanceKey // Update PI key on trigger if it wasn't set
             }
 
             "CANCELED" -> {
                 entity.state = TimerState.CANCELED
                 entity.endTime = record.metadata.timestamp
             }
         }
 
         entity.repetitions = record.repetitions
         // Update position and timestamp on state changes? Original didn't, keep consistent for now.
         // entity.position = record.metadata.position
         // entity.timestamp = record.metadata.timestamp
 
         timerRepository.save(entity)
         // Consider adding a dedicated onTimerUpdated publisher event if the UI needs it
         // dataUpdatesPublisher.onTimerUpdated(entity)
     }
 
     private fun createTimer(record: Schema.TimerRecord): Timer {
         return Timer(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Timer record position
             dueDate = record.dueDate,
             repetitions = record.repetitions,
             elementId = record.targetElementId,
             // Use .takeIf { it > 0 } only if these keys can be 0 in the record and should be null in DB
             processDefinitionKey = record.processDefinitionKey.takeIf { it > 0 },
             processInstanceKey = record.processInstanceKey.takeIf { it > 0 },
             elementInstanceKey = record.elementInstanceKey.takeIf { it > 0 }
         );
     }
 
     private fun importMessageRecord(record: Schema.MessageRecord) {
         // Message events can be relevant for UI. Save and publish immediately (within the message's partition thread)
         importMessage(record)
 
         // Message Variables are likely high volume, add to batcher?
         // Original code imports variables only on PUBLISHED. Keep this logic.
         if (record.metadata.intent == "PUBLISHED") {
             importMessageVariables(record) // This adds to batcher if implemented
         }
     }
 
     private fun importMessage(record: Schema.MessageRecord) {
         // Message entity is relevant for UI, save and publish immediately (within the message's partition thread)
         val entity = messageRepository
             .findById(record.metadata.key)
             .orElse(createMessage(record))
 
         when (record.metadata.intent) {
             "PUBLISHED" -> entity.state = MessageState.PUBLISHED
             "EXPIRED" -> entity.state = MessageState.EXPIRED
             // Add other states if needed (e.g., CANCELED?)
         }
 
         entity.timestamp = record.metadata.timestamp // Update timestamp on state changes
 
         messageRepository.save(entity)
         // Consider adding a dedicated onMessageUpdated publisher event
         // dataUpdatesPublisher.onMessageUpdated(entity)
     }
 
     private fun createMessage(record: Schema.MessageRecord): Message {
         return Message(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Message record position
             name = record.name,
             correlationKey = record.correlationKey.takeIf { it.isNotEmpty() },
             messageId = record.messageId.takeIf { it.isNotEmpty() },
             timeToLive = record.timeToLive
         );
     }
 
     private fun importMessageVariables(record: Schema.MessageRecord) {
         // Message Variables can be high volume, add to batcher if needed.
         // The current approach calculates ID and saves individually. Let's batch them.
         val messageKey = record.metadata.key
         val messagePosition = record.metadata.position
 
         structToMap(record.variables).forEach { (name, value) ->
             val id = messageKey.toString() + "_" + name // Use underscore for clarity in composite ID
 
             // Note: This finds or creates in the thread, then adds to batcher buffer.
             // The actual save happens later in the batcher's thread.
             // Consider if MessageVariable updates need to be tracked separately or just the latest value.
             // Current entity structure seems to only store the latest value per (messageKey, name) implicitly via findById.
             // If history is needed, VariableUpdate pattern would be needed here too.
             // Assuming only latest value is needed, the batcher for MessageVariable entity makes sense.
              val entity = messageVariableRepository // Use repository to check if exists
                  .findById(id)
                  .orElse(
                      MessageVariable(
                          id = id,
                          name = name,
                          value = value,
                          messageKey = messageKey,
                          position = messagePosition // Position of the message record that brought this variable
                      )
                  )
              // Update value and position if entity exists (shouldn't happen for MessageVariables based on ID scheme?)
              // entity.value = value // Only needed if variables for the same messageKey+name can be updated by subsequent records (unlikely)
              // entity.position = messagePosition // Update position to the latest record's position
 
             // We need a batcher for MessageVariable
             // messageVariableBatcher.add(entity) // Requires adding messageVariableBatcher
             messageVariableRepository.save(entity) // For now, keep individual save if not adding MessageVariableBatcher
         }
          // If you add messageVariableBatcher, remove the save call here.
          // If you add messageVariableBatcher, stop it in the stop() method.
     }
 
     private fun importMessageStartEventSubscriptionRecord(record: Schema.MessageStartEventSubscriptionRecord) {
         // Message Subscription events are relevant for UI. Save and publish immediately (within the subscription's partition thread)
         val entity = messageSubscriptionRepository
             .findById(record.metadata.key)
             .orElse(createMessageSubscription(record))
 
         when (record.metadata.intent) {
             "CREATED" -> entity.state = MessageSubscriptionState.CREATED
             "CORRELATED" -> {
                 entity.state = MessageSubscriptionState.CORRELATED
                 // Correlation events are also relevant, save immediately?
                 importMessageCorrelation(record) // This saves MessageCorrelation immediately
             }
 
             "DELETED" -> entity.state = MessageSubscriptionState.DELETED
         }
 
         entity.timestamp = record.metadata.timestamp
 
         messageSubscriptionRepository.save(entity)
         // Consider adding a dedicated onMessageSubscriptionUpdated publisher event
         // dataUpdatesPublisher.onMessageSubscriptionUpdated(entity)
     }
 
     private fun createMessageSubscription(record: Schema.MessageStartEventSubscriptionRecord): MessageSubscription {
         return MessageSubscription(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Subscription record position
             messageName = record.messageName,
             processDefinitionKey = record.processDefinitionKey,
             elementId = record.startEventId,
             elementInstanceKey = null, // Start event subscriptions are not tied to a specific element instance
             processInstanceKey = null, // Start event subscriptions are not tied to a specific process instance initially
             messageCorrelationKey = null // No correlation key for start events
         );
     }
 
     private fun importProcessMessageSubscriptionRecord(record: Schema.ProcessMessageSubscriptionRecord) {
         // Process Message Subscription events are relevant for UI. Save and publish immediately (within the subscription's partition thread)
         val entity = messageSubscriptionRepository
             .findById(record.metadata.key)
             .orElse(createMessageSubscription(record))
 
         when (record.metadata.intent) {
             "CREATING" -> entity.state = MessageSubscriptionState.CREATING
             "CREATED" -> entity.state = MessageSubscriptionState.CREATED
             "CORRELATING" -> entity.state = MessageSubscriptionState.CORRELATING
             "CORRELATED" -> {
                 entity.state = MessageSubscriptionState.CORRELATED
                  importMessageCorrelation(record) // This saves MessageCorrelation immediately
             }
 
             "REJECTED" -> entity.state = MessageSubscriptionState.REJECTED
             "DELETED" -> entity.state = MessageSubscriptionState.DELETED
         }
 
         entity.timestamp = record.metadata.timestamp
 
         messageSubscriptionRepository.save(entity)
         // Consider adding a dedicated onMessageSubscriptionUpdated publisher event
         // dataUpdatesPublisher.onMessageSubscriptionUpdated(entity)
     }
 
     private fun createMessageSubscription(record: Schema.ProcessMessageSubscriptionRecord): MessageSubscription {
         return MessageSubscription(
             key = record.metadata.key,
             position = record.metadata.position, // Position here is the Subscription record position
             messageName = record.messageName,
             messageCorrelationKey = record.correlationKey,
             processInstanceKey = record.processInstanceKey,
             elementInstanceKey = record.elementInstanceKey,
             elementId = record.elementId, // Element Id of the catch event/task
             processDefinitionKey = null // Process message subscriptions are tied to a specific process instance, not definition
         );
     }
 
     // Message Correlation can be high volume, could be batched.
     // For now, keeping individual save as per original logic, happens within the subscription's partition thread.
     private fun importMessageCorrelation(record: Schema.ProcessMessageSubscriptionRecord) {
         val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
         val entity = messageCorrelationRepository
             .findById(partitionIdWithPosition)
             .orElse(
                 MessageCorrelation(
                     partitionIdWithPosition = partitionIdWithPosition, // Unique ID for this correlation event
                     messageKey = record.messageKey,
                     messageName = record.messageName,
                     elementInstanceKey = record.elementInstanceKey,
                     processInstanceKey = record.processInstanceKey,
                     elementId = record.elementId, // Element Id of the catch event/task
                     processDefinitionKey = null, // Correlation is instance-specific
                     timestamp = record.metadata.timestamp
                 )
             )
 
         messageCorrelationRepository.save(entity)
          // Consider adding a dedicated onMessageCorrelationCreated publisher event
         // dataUpdatesPublisher.onMessageCorrelationCreated(entity)
     }
 
     // Message Correlation for Start Event can be high volume, could be batched.
     // For now, keeping individual save as per original logic, happens within the subscription's partition thread.
     private fun importMessageCorrelation(record: Schema.MessageStartEventSubscriptionRecord) {
         val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
         val entity = messageCorrelationRepository
             .findById(partitionIdWithPosition)
             .orElse(
                 MessageCorrelation(
                     partitionIdWithPosition = partitionIdWithPosition, // Unique ID for this correlation event
                     messageKey = record.messageKey,
                     messageName = record.messageName,
                     elementInstanceKey = null, // Not tied to a specific element instance for start events
                     processInstanceKey = record.processInstanceKey,
                     elementId = record.startEventId, // Element Id of the start event
                     processDefinitionKey = record.processDefinitionKey, // Correlation for start event is definition-specific
                     timestamp = record.metadata.timestamp
                 )
             )
 
         messageCorrelationRepository.save(entity)
          // Consider adding a dedicated onMessageCorrelationCreated publisher event
         // dataUpdatesPublisher.onMessageCorrelationCreated(entity)
     }
 
     private fun importError(record: Schema.ErrorRecord) {
         // Error is critical for UI/monitoring. Save and publish immediately (within the error's partition thread)
         val entity = errorRepository.findById(record.metadata.position)
             .orElse(Error(
                 position = record.metadata.position, // Error entity uses position as key
                 errorEventPosition = record.errorEventPosition,
                 exceptionMessage = record.exceptionMessage,
                 stacktrace = record.stacktrace,
                 processInstanceKey = record.processInstanceKey.takeIf { it > 0 }
             ))
          // Assuming Error entity needs a timestamp field. If not, remove.
          // entity.timestamp = record.metadata.timestamp // Update if exists
 
         errorRepository.save(entity)
         // Consider adding a dedicated onErrorCreated publisher event
         // dataUpdatesPublisher.onErrorCreated(entity)
     }
 }
 
 // Utility for hybrid time+size batching
 class Batcher<T>(
     private val maxSize: Int,
     private val maxDelayMs: Long,
     private val flushAction: (List<T>) -> Unit
 ) {
     private val buffer = Collections.synchronizedList(mutableListOf<T>())
     private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
     @Volatile private var running = true // Flag to indicate if batcher is active
 
     init {
         // Schedule the time-based flush task
         scheduler.scheduleAtFixedRate({
             if (running) { // Only flush if the batcher is running
                  try {
                      flushIfNeeded(force = true)
                  } catch (e: Exception) {
                      // Log the error but don't rethrow to keep the scheduler running
                      println("Error flushing batch: ${e.message}")
                      e.printStackTrace() // Log full stack trace for debugging
                  }
             }
         }, maxDelayMs, maxDelayMs, TimeUnit.MILLISECONDS)
     }
 
     fun add(item: T) {
         if (!running) {
             println("Batcher is stopped, item not added.")
             return // Don't add if batcher is stopped
         }
         synchronized(buffer) { // Synchronize on buffer for thread-safe access
              buffer.add(item)
              if (buffer.size >= maxSize) {
                  flushIfNeeded()
              }
         }
     }
 
     @Synchronized // Synchronize the flush logic
     private fun flushIfNeeded(force: Boolean = false) {
          if (buffer.isNotEmpty() && (buffer.size >= maxSize || force)) {
              val itemsToFlush = ArrayList(buffer) // Create a copy for flushing
              buffer.clear() // Clear the buffer immediately
 
              if (itemsToFlush.isNotEmpty()) {
                   try {
                      flushAction(itemsToFlush) // Perform the batch save
                      // println("Flushed batch of ${itemsToFlush.size} items.")
                   } catch (e: Exception) {
                      // Log the error. In a real system, you might want to retry
                      // or move items to a dead-letter queue to avoid data loss.
                      println("Error during batch flush: ${e.message}")
                      e.printStackTrace()
                      // Optionally, decide whether to add items back to buffer or handle error
                      // For simplicity, we just log and move on, risking data loss on error.
                   }
              }
          }
     }
 
     fun stop() {
         running = false // Stop accepting new items
         println("Stopping batcher...")
         scheduler.shutdown() // Stop the scheduled flush task
         try {
             // Wait a reasonable time for the last scheduled task to finish
              if (!scheduler.awaitTermination(maxDelayMs * 2, TimeUnit.MILLISECONDS)) {
                  println("Batcher scheduler did not terminate gracefully, forcing shutdown.")
                  scheduler.shutdownNow()
              }
         } catch (e: InterruptedException) {
              println("Batcher scheduler shutdown interrupted.")
              scheduler.shutdownNow()
              Thread.currentThread().interrupt()
         }
         // Perform a final forced flush to ensure any remaining items are saved
         println("Performing final batch flush...")
         flushIfNeeded(force = true)
         println("Batcher stopped.")
     }
 }