package io.zeebe.zeeqs.data.repository

import io.zeebe.zeeqs.data.entity.Variable
import org.springframework.data.jpa.repository.QueryHints
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import javax.persistence.QueryHint

@Repository
interface VariableRepository : PagingAndSortingRepository<Variable, Long> {

    @Transactional(readOnly = true)
    @QueryHints(value = [QueryHint(name = "org.hibernate.fetchSize", value = "1000")])
    fun findByProcessInstanceKey(processInstanceKey: Long): List<Variable>

    @Transactional(readOnly = true)
    fun findByScopeKey(scopeKey: Long): List<Variable>

    @Transactional(readOnly = true)
    fun findByProcessInstanceKeyInAndName(processInstanceKey: List<Long>, name: String): List<Variable>


    @Transactional(readOnly = true)
    fun findByProcessInstanceKeyInAndNameIn(processInstanceKey: List<Long>, name: List<String>): List<Variable>

}