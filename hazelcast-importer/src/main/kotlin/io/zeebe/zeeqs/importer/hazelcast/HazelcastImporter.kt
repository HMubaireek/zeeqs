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
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

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
    private val dataUpdatesPublisher: DataUpdatesPublisher
) {

    private val logger = LoggerFactory.getLogger(HazelcastImporter::class.java)

    var zeebeHazelcast: ZeebeHazelcast? = null

    fun start(hazelcastProperties: HazelcastProperties) {
        logger.info("HazelcastImporter starting - Version with key+processInstanceKey composite lookup fix")

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

        val updateSequence: ((Long) -> Unit) = {
            hazelcastConfig.sequence = it
            hazelcastConfigRepository.save(hazelcastConfig)
        }

        val clientConfig = ClientConfig()
        val networkConfig = clientConfig.networkConfig
        networkConfig.addresses = listOf(hazelcastConnection)

        val connectionRetryConfig = clientConfig.connectionStrategyConfig.connectionRetryConfig
        connectionRetryConfig.clusterConnectTimeoutMillis = hazelcastConnectionTimeout.toMillis()
        // These retry configs can be user-configured in application.yml
        connectionRetryConfig.initialBackoffMillis =
            hazelcastConnectionInitialBackoff.toMillis().toInt()
        connectionRetryConfig.multiplier = hazelcastConnectionBackoffMultiplier
        connectionRetryConfig.maxBackoffMillis = hazelcastConnectionMaxBackoff.toMillis().toInt()

        val hazelcast = HazelcastClient.newHazelcastClient(clientConfig)

        val builder = ZeebeHazelcast.newBuilder(hazelcast).name(hazelcastRingbuffer)
            .addProcessListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }?.let(this::importProcess)
            }
            .addProcessInstanceListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importProcessInstanceRecord)
            }
            .addVariableListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importVariableRecord)
            }
            .addJobListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }?.let(this::importJobRecord)
            }
            .addIncidentListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importIncidentRecord)
            }
            .addTimerListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importTimerRecord)
            }
            .addMessageListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importMessageRecord)
            }
            .addMessageStartEventSubscriptionListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importMessageStartEventSubscriptionRecord)
            }
            .addProcessMessageSubscriptionListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(this::importProcessMessageSubscriptionRecord)
            }
            .addDecisionListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(decisionEvaluationImporter::importDecision)
            }
            .addDecisionRequirementsListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(decisionEvaluationImporter::importDecisionRequirements)
            }
            .addDecisionEvaluationListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(decisionEvaluationImporter::importDecisionEvaluation)
            }
            .addSignalListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(signalImporter::importSignal)
            }
            .addSignalSubscriptionListener {
                it.takeIf { it.metadata.recordType == RecordType.EVENT }
                    ?.let(signalImporter::importSignalSubscription)
            }
            .addErrorListener(this::importError)
            .postProcessListener(updateSequence)

        if (hazelcastConfig.sequence >= 0) {
            builder.readFrom(hazelcastConfig.sequence)
        } else {
            builder.readFromHead()
        }

        zeebeHazelcast = builder.build()
    }

    fun stop() {
        zeebeHazelcast?.close()
    }

    private fun getPartitionIdWithPosition(metadata: Schema.RecordMetadata) =
        "${metadata.partitionId}-${metadata.position}"

    private fun importProcess(process: Schema.ProcessRecord) {
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

    private fun importProcessInstanceRecord(record: Schema.ProcessInstanceRecord) {
        if (record.processInstanceKey == record.metadata.key) {
            importProcessInstance(record)
        }

        importElementInstance(record)
        importElementInstanceStateTransition(record)
    }

    private fun importProcessInstance(record: Schema.ProcessInstanceRecord) {
        val entity = processInstanceRepository
            .findById(record.processInstanceKey)
            .orElse(createProcessInstance(record))

        when (record.metadata.intent) {
            "ELEMENT_ACTIVATED" -> {
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
            position = record.metadata.position,
            bpmnProcessId = record.bpmnProcessId,
            version = record.version,
            processDefinitionKey = record.processDefinitionKey,
            parentProcessInstanceKey = record.parentProcessInstanceKey.takeIf { it > 0 },
            parentElementInstanceKey = record.parentElementInstanceKey.takeIf { it > 0 }
        )
    }

    private fun importElementInstance(record: Schema.ProcessInstanceRecord) {
        logger.debug(
            "importElementInstance - Processing: key={}, elementId={}, PI={}, intent={}, bpmnType={}",
            record.metadata.key, record.elementId, record.processInstanceKey, 
            record.metadata.intent, record.bpmnElementType
        )
        
        // Look up by BOTH key AND processInstanceKey to handle Zeebe key reuse correctly
        val existingEntity = elementInstanceRepository.findByKeyAndProcessInstanceKey(
            record.metadata.key, 
            record.processInstanceKey
        )
        
        val entity = if (existingEntity != null) {
            logger.debug(
                "importElementInstance - Found existing entity: key={}, elementId={}, PI={}, state={}",
                existingEntity.key, existingEntity.elementId, existingEntity.processInstanceKey, existingEntity.state
            )
            existingEntity
        } else {
            // New element instance
            logger.debug(
                "importElementInstance - Creating new element instance: key={}, elementId={}, PI={}",
                record.metadata.key, record.elementId, record.processInstanceKey
            )
            createElementInstance(record)
        }

        entity.state = getElementInstanceState(record)
        
        logger.debug(
            "importElementInstance - Saving: key={}, elementId={}, PI={}, state={}",
            entity.key, entity.elementId, entity.processInstanceKey, entity.state
        )

        when (record.metadata.intent) {
            "ELEMENT_ACTIVATING" -> {
                entity.startTime = record.metadata.timestamp
            }

            "ELEMENT_COMPLETED", "ELEMENT_TERMINATED" -> {
                entity.endTime = record.metadata.timestamp
            }

            "SEQUENCE_FLOW_TAKEN" -> {
                entity.startTime = record.metadata.timestamp
                entity.endTime = record.metadata.timestamp
            }
        }

        elementInstanceRepository.save(entity)
        dataUpdatesPublisher.onElementInstanceUpdated(entity)
        
        logger.debug(
            "importElementInstance - Saved successfully: key={}, elementId={}, PI={}",
            entity.key, entity.elementId, entity.processInstanceKey
        )
    }
    
    private fun mapBpmnElementType(bpmnElementTypeString: String): BpmnElementType {
        return when (bpmnElementTypeString) {
            "UNSPECIFIED" -> BpmnElementType.UNSPECIFIED
            "BOUNDARY_EVENT" -> BpmnElementType.BOUNDARY_EVENT
            "CALL_ACTIVITY" -> BpmnElementType.CALL_ACTIVITY
            "END_EVENT" -> BpmnElementType.END_EVENT
            "EVENT_BASED_GATEWAY" -> BpmnElementType.EVENT_BASED_GATEWAY
            "EXCLUSIVE_GATEWAY" -> BpmnElementType.EXCLUSIVE_GATEWAY
            "INTERMEDIATE_CATCH_EVENT" -> BpmnElementType.INTERMEDIATE_CATCH_EVENT
            "INTERMEDIATE_THROW_EVENT" -> BpmnElementType.INTERMEDIATE_THROW_EVENT
            "PARALLEL_GATEWAY" -> BpmnElementType.PARALLEL_GATEWAY
            "PROCESS" -> BpmnElementType.PROCESS
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
    }
    
    private fun createElementInstance(record: Schema.ProcessInstanceRecord): ElementInstance {
        val bpmnElementType = mapBpmnElementType(record.bpmnElementType)

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
        return when (record.metadata.intent) {
            "ELEMENT_ACTIVATING" -> ElementInstanceState.ACTIVATING
            "ELEMENT_ACTIVATED" -> ElementInstanceState.ACTIVATED
            "ELEMENT_COMPLETING" -> ElementInstanceState.COMPLETING
            "ELEMENT_COMPLETED" -> ElementInstanceState.COMPLETED
            "ELEMENT_TERMINATING" -> ElementInstanceState.TERMINATING
            "ELEMENT_TERMINATED" -> ElementInstanceState.TERMINATED
            "SEQUENCE_FLOW_TAKEN" -> ElementInstanceState.TAKEN
            else -> ElementInstanceState.ACTIVATING
        }
    }

    private fun importElementInstanceStateTransition(record: Schema.ProcessInstanceRecord) {

        val state = getElementInstanceState(record)

        val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
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

        elementInstanceStateTransitionRepository.save(entity)
    }

    private fun importVariableRecord(record: Schema.VariableRecord) {
        importVariable(record)
        importVariableUpdate(record)
    }

    private fun importVariable(record: Schema.VariableRecord) {

        val entity = variableRepository
            .findById(record.metadata.key)
            .orElse(createVariable(record))

        entity.value = record.value
        entity.timestamp = record.metadata.timestamp

        variableRepository.save(entity)
        dataUpdatesPublisher.onVariableUpdated(entity)
    }

    private fun createVariable(record: Schema.VariableRecord): Variable {
        return Variable(
            key = record.metadata.key,
            position = record.metadata.position,
            name = record.name,
            value = record.value,
            processInstanceKey = record.processInstanceKey,
            processDefinitionKey = record.processDefinitionKey,
            scopeKey = record.scopeKey,
            timestamp = record.metadata.timestamp
        )
    }

    private fun importVariableUpdate(record: Schema.VariableRecord) {

        val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
        val entity = variableUpdateRepository
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

        variableUpdateRepository.save(entity)
    }

    private fun importJobRecord(record: Schema.JobRecord) {
        if (isJobForUserTask(record)) {
            importUserTask(record)
        } else {
            importJobForWorker(record)
        }
    }

    private fun isJobForUserTask(record: Schema.JobRecord) =
        record.type == Protocol.USER_TASK_JOB_TYPE

    private fun importJobForWorker(record: Schema.JobRecord) {
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
        entity.timestamp = record.metadata.timestamp

        jobRepository.save(entity)
        dataUpdatesPublisher.onJobUpdated(entity)
    }

    private fun createJob(record: Schema.JobRecord): Job {
        return Job(
            key = record.metadata.key,
            position = record.metadata.position,
            jobType = record.type,
            processInstanceKey = record.processInstanceKey,
            elementInstanceKey = record.elementInstanceKey,
            processDefinitionKey = record.processDefinitionKey
        )
    }

    private fun importUserTask(record: Schema.JobRecord) {
        logger.debug(
            "importUserTask - Processing: jobKey={}, elementInstanceKey={}, PI={}, intent={}",
            record.metadata.key, record.elementInstanceKey, record.processInstanceKey, record.metadata.intent
        )
        
        val entity = userTaskRepository
            .findById(record.metadata.key)
            .orElse(createUserTask(record))

        logger.info(
            "importUserTask - UserTask entity: key={}, elementInstanceKey={}, PI={}, intent={}",
            entity.key, entity.elementInstanceKey, entity.processInstanceKey, record.metadata.intent
        )
        
        // Look up ElementInstance by BOTH key AND processInstanceKey (correct way)
        val elementInstance = elementInstanceRepository.findByKeyAndProcessInstanceKey(
            record.elementInstanceKey,
            record.processInstanceKey
        )
        
        if (elementInstance != null) {
            logger.info(
                "importUserTask - Found ElementInstance: key={}, elementId={}, PI={}, bpmnType={}",
                elementInstance.key, elementInstance.elementId, elementInstance.processInstanceKey, 
                elementInstance.bpmnElementType
            )
        } else {
            logger.warn(
                "importUserTask - ElementInstance NOT FOUND for key={}, PI={} (UserTask key={})",
                record.elementInstanceKey, record.processInstanceKey, record.metadata.key
            )
        }

        when (record.metadata.intent) {
            "CREATED" -> {
                entity.startTime = record.metadata.timestamp
                logger.info(
                    "importUserTask - UserTask CREATED: key={}, elementInstanceKey={}, PI={}",
                    entity.key, entity.elementInstanceKey, entity.processInstanceKey
                )
            }

            "COMPLETED" -> {
                entity.state = UserTaskState.COMPLETED
                entity.endTime = record.metadata.timestamp
                logger.info(
                    "importUserTask - UserTask COMPLETED: key={}, elementInstanceKey={}, PI={}",
                    entity.key, entity.elementInstanceKey, entity.processInstanceKey
                )
            }

            "CANCELED" -> {
                entity.state = UserTaskState.CANCELED
                entity.endTime = record.metadata.timestamp
                logger.info(
                    "importUserTask - UserTask CANCELED: key={}, elementInstanceKey={}, PI={}",
                    entity.key, entity.elementInstanceKey, entity.processInstanceKey
                )
            }
        }

        entity.timestamp = record.metadata.timestamp

        userTaskRepository.save(entity)
        
        logger.debug(
            "importUserTask - Saved: key={}, elementInstanceKey={}, PI={}, state={}",
            entity.key, entity.elementInstanceKey, entity.processInstanceKey, entity.state
        )
    }

    private fun createUserTask(record: Schema.JobRecord): UserTask {
        val customHeaders = record.customHeaders.fieldsMap
        val assignee = customHeaders[Protocol.USER_TASK_ASSIGNEE_HEADER_NAME]?.stringValue
        val candidateGroups =
            customHeaders[Protocol.USER_TASK_CANDIDATE_GROUPS_HEADER_NAME]?.stringValue
        val formKey = customHeaders[Protocol.USER_TASK_FORM_KEY_HEADER_NAME]?.stringValue
        return UserTask(
            key = record.metadata.key,
            position = record.metadata.position,
            processInstanceKey = record.processInstanceKey,
            processDefinitionKey = record.processDefinitionKey,
            elementInstanceKey = record.elementInstanceKey,
            assignee = assignee,
            candidateGroups = candidateGroups,
            formKey = formKey
        )
    }

    private fun importIncidentRecord(record: Schema.IncidentRecord) {
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
        dataUpdatesPublisher.onIncidentUpdated(entity)
    }

    private fun createIncident(record: Schema.IncidentRecord): Incident {
        return Incident(
            key = record.metadata.key,
            position = record.metadata.position,
            errorType = record.errorType,
            errorMessage = record.errorMessage,
            processInstanceKey = record.processInstanceKey,
            processDefinitionKey = record.processDefinitionKey,
            elementInstanceKey = record.elementInstanceKey,
            jobKey = record.jobKey.takeIf { it > 0 }
        )
    }

    private fun importTimerRecord(record: Schema.TimerRecord) {
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
                entity.processInstanceKey = record.processInstanceKey
            }

            "CANCELED" -> {
                entity.state = TimerState.CANCELED
                entity.endTime = record.metadata.timestamp
            }
        }

        entity.repetitions = record.repetitions

        timerRepository.save(entity)
    }

    private fun createTimer(record: Schema.TimerRecord): Timer {
        return Timer(
            key = record.metadata.key,
            position = record.metadata.position,
            dueDate = record.dueDate,
            repetitions = record.repetitions,
            elementId = record.targetElementId,
            processDefinitionKey = record.processDefinitionKey.takeIf { it > 0 },
            processInstanceKey = record.processInstanceKey.takeIf { it > 0 },
            elementInstanceKey = record.elementInstanceKey.takeIf { it > 0 }
        );
    }

    private fun importMessageRecord(record: Schema.MessageRecord) {
        importMessage(record)

        if (record.metadata.intent == "PUBLISHED") {
            importMessageVariables(record)
        }
    }

    private fun importMessage(record: Schema.MessageRecord) {
        val entity = messageRepository
            .findById(record.metadata.key)
            .orElse(createMessage(record))

        when (record.metadata.intent) {
            "PUBLISHED" -> entity.state = MessageState.PUBLISHED
            "EXPIRED" -> entity.state = MessageState.EXPIRED
        }

        entity.timestamp = record.metadata.timestamp

        messageRepository.save(entity)
    }

    private fun createMessage(record: Schema.MessageRecord): Message {
        return Message(
            key = record.metadata.key,
            position = record.metadata.position,
            name = record.name,
            correlationKey = record.correlationKey.takeIf { it.isNotEmpty() },
            messageId = record.messageId.takeIf { it.isNotEmpty() },
            timeToLive = record.timeToLive
        );
    }

    private fun importMessageVariables(record: Schema.MessageRecord) {
        val messageKey = record.metadata.key
        val messagePosition = record.metadata.position

        structToMap(record.variables).forEach { (name, value) ->
            val id = messageKey.toString() + name

            val entity = messageVariableRepository
                .findById(id)
                .orElse(
                    MessageVariable(
                        id = id,
                        name = name,
                        value = value,
                        messageKey = messageKey,
                        position = messagePosition
                    )
                )

            messageVariableRepository.save(entity)
        }
    }

    private fun importMessageStartEventSubscriptionRecord(record: Schema.MessageStartEventSubscriptionRecord) {
        val entity = messageSubscriptionRepository
            .findById(record.metadata.key)
            .orElse(createMessageSubscription(record))

        when (record.metadata.intent) {
            "CREATED" -> entity.state = MessageSubscriptionState.CREATED
            "CORRELATED" -> {
                entity.state = MessageSubscriptionState.CORRELATED
                importMessageCorrelation(record)
            }

            "DELETED" -> entity.state = MessageSubscriptionState.DELETED
        }

        entity.timestamp = record.metadata.timestamp

        messageSubscriptionRepository.save(entity)
    }

    private fun createMessageSubscription(record: Schema.MessageStartEventSubscriptionRecord): MessageSubscription {
        return MessageSubscription(
            key = record.metadata.key,
            position = record.metadata.position,
            messageName = record.messageName,
            processDefinitionKey = record.processDefinitionKey,
            elementId = record.startEventId,
            elementInstanceKey = null,
            processInstanceKey = null,
            messageCorrelationKey = null
        );
    }

    private fun importProcessMessageSubscriptionRecord(record: Schema.ProcessMessageSubscriptionRecord) {
        val entity = messageSubscriptionRepository
            .findById(record.metadata.key)
            .orElse(createMessageSubscription(record))

        when (record.metadata.intent) {
            "CREATING" -> entity.state = MessageSubscriptionState.CREATING
            "CREATED" -> entity.state = MessageSubscriptionState.CREATED
            "CORRELATING" -> entity.state = MessageSubscriptionState.CORRELATING
            "CORRELATED" -> {
                entity.state = MessageSubscriptionState.CORRELATED

                importMessageCorrelation(record)
            }

            "REJECTED" -> entity.state = MessageSubscriptionState.REJECTED
            "DELETED" -> entity.state = MessageSubscriptionState.DELETED
        }

        entity.timestamp = record.metadata.timestamp

        messageSubscriptionRepository.save(entity)
    }

    private fun createMessageSubscription(record: Schema.ProcessMessageSubscriptionRecord): MessageSubscription {
        return MessageSubscription(
            key = record.metadata.key,
            position = record.metadata.position,
            messageName = record.messageName,
            messageCorrelationKey = record.correlationKey,
            processInstanceKey = record.processInstanceKey,
            elementInstanceKey = record.elementInstanceKey,
            elementId = record.elementId,
            processDefinitionKey = null
        );
    }

    private fun importMessageCorrelation(record: Schema.ProcessMessageSubscriptionRecord) {

        val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
        val entity = messageCorrelationRepository
            .findById(partitionIdWithPosition)
            .orElse(
                MessageCorrelation(
                    partitionIdWithPosition = partitionIdWithPosition,
                    messageKey = record.messageKey,
                    messageName = record.messageName,
                    elementInstanceKey = record.elementInstanceKey,
                    processInstanceKey = record.processInstanceKey,
                    elementId = record.elementId,
                    processDefinitionKey = null,
                    timestamp = record.metadata.timestamp
                )
            )

        messageCorrelationRepository.save(entity)
    }

    private fun importMessageCorrelation(record: Schema.MessageStartEventSubscriptionRecord) {

        val partitionIdWithPosition = getPartitionIdWithPosition(record.metadata)
        val entity = messageCorrelationRepository
            .findById(partitionIdWithPosition)
            .orElse(
                MessageCorrelation(
                    partitionIdWithPosition = partitionIdWithPosition,
                    messageKey = record.messageKey,
                    messageName = record.messageName,
                    elementInstanceKey = null,
                    processInstanceKey = record.processInstanceKey,
                    elementId = record.startEventId,
                    processDefinitionKey = record.processDefinitionKey,
                    timestamp = record.metadata.timestamp
                )
            )

        messageCorrelationRepository.save(entity)
    }

    private fun importError(record: Schema.ErrorRecord) {

        val entity = errorRepository.findById(record.metadata.position)
            .orElse(Error(
                position = record.metadata.position,
                errorEventPosition = record.errorEventPosition,
                exceptionMessage = record.exceptionMessage,
                stacktrace = record.stacktrace,
                processInstanceKey = record.processInstanceKey.takeIf { it > 0 }
            ))

        errorRepository.save(entity)
    }


}
