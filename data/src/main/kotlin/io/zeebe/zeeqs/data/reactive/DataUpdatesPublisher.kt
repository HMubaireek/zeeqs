package io.zeebe.zeeqs.data.reactive

import io.zeebe.zeeqs.data.entity.*
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Consumer

@Component
class DataUpdatesPublisher {

    private val processListeners = CopyOnWriteArrayList<Consumer<Process>>()
    private val decisionListeners = CopyOnWriteArrayList<Consumer<Decision>>()
    private val processInstanceListeners = CopyOnWriteArrayList<Consumer<ProcessInstance>>()
    private val elementInstanceListeners = CopyOnWriteArrayList<Consumer<ElementInstance>>()
    private val variableListeners = CopyOnWriteArrayList<Consumer<Variable>>()
    private val incidentListeners = CopyOnWriteArrayList<Consumer<Incident>>()
    private val jobListeners = CopyOnWriteArrayList<Consumer<Job>>()
    private val decisionEvaluationListeners = CopyOnWriteArrayList<Consumer<DecisionEvaluation>>()

    fun onProcessUpdated(process: Process) {
        // Quick exit if no listeners (most common case) - Performance optimization
        if (processListeners.isEmpty()) return
        processListeners.forEach { it.accept(process) }
    }

    fun onDecisionUpdated(decision: Decision) {
        if (decisionListeners.isEmpty()) return
        decisionListeners.forEach { it.accept(decision) }
    }

    fun onProcessInstanceUpdated(processInstance: ProcessInstance) {
        if (processInstanceListeners.isEmpty()) return
        processInstanceListeners.forEach { it.accept(processInstance) }
    }

    fun onElementInstanceUpdated(elementInstance: ElementInstance) {
        if (elementInstanceListeners.isEmpty()) return
        elementInstanceListeners.forEach { it.accept(elementInstance) }
    }

    fun onVariableUpdated(variable: Variable) {
        if (variableListeners.isEmpty()) return
        variableListeners.forEach { it.accept(variable) }
    }

    fun onIncidentUpdated(incident: Incident) {
        if (incidentListeners.isEmpty()) return
        incidentListeners.forEach { it.accept(incident) }
    }

    fun onJobUpdated(job: Job) {
        if (jobListeners.isEmpty()) return
        jobListeners.forEach { it.accept(job) }
    }

    fun onDecisionEvaluationUpdated(decisionEvaluation: DecisionEvaluation) {
        if (decisionEvaluationListeners.isEmpty()) return
        decisionEvaluationListeners.forEach { it.accept(decisionEvaluation) }
    }

    fun registerProcessListener(listener: Consumer<Process>) {
        processListeners.add(listener)
    }

    fun registerDecisionListener(listener: Consumer<Decision>) {
        decisionListeners.add(listener)
    }

    fun registerProcessInstanceListener(listener: Consumer<ProcessInstance>) {
        processInstanceListeners.add(listener)
    }

    fun registerElementInstanceListener(listener: Consumer<ElementInstance>) {
        elementInstanceListeners.add(listener)
    }

    fun registerVariableListener(listener: Consumer<Variable>) {
        variableListeners.add(listener)
    }

    fun registerIncidentListener(listener: Consumer<Incident>) {
        incidentListeners.add(listener)
    }

    fun registerJobListener(listener: Consumer<Job>) {
        jobListeners.add(listener)
    }

    fun registerDecisionEvaluationListener(listener: Consumer<DecisionEvaluation>) {
        decisionEvaluationListeners.add(listener)
    }
}