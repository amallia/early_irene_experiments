package edu.umass.cics.ciir

import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.StreamingStats
import java.util.*
import kotlin.streams.asSequence

/**
 *
 * @author jfoley.
 */

sealed class Op() {
    abstract val toCode: String
    abstract fun exec(vm: VMState)
}
data class AddOp(val lhs: Int, val rhs: Int, val dest: Int) : Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = vm.mem[lhs] + vm.mem[rhs] }
    override val toCode: String = "vars[$dest] = vars[$lhs] + vars[$rhs]"
}
data class MultOp(val lhs: Int, val rhs: Int, val dest: Int) : Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = vm.mem[lhs] * vm.mem[rhs] }
    override val toCode: String = "vars[$dest] = vars[$lhs] * vars[$rhs]"
}
data class LogDivOp(val lhs: Int, val rhs: Int, val dest: Int): Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = Math.log(vm.mem[lhs] / vm.mem[rhs]) }
    override val toCode: String = "vars[$dest] = Math.log(vars[$lhs] / vars[$rhs])"
}
data class DivOp(val lhs: Int, val rhs: Int, val dest: Int) : Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = (vm.mem[lhs] / vm.mem[rhs]) }
    override val toCode: String = "vars[$dest] = vars[$lhs] / vars[$rhs]"
}
data class ReturnOp(val src: Int): Op() {
    override fun exec(vm: VMState) { vm.output = vm.mem[src]; vm.done = true; }
    override val toCode: String = "return vars[$src]"
}
data class LogOp(val src: Int, val dest: Int = src) : Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = Math.log(vm.mem[src]) }
    override val toCode: String = "vars[$dest] = Math.log(vars[$src])"
}
data class LoadConst(val dest: Int, val x: Double): Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = x }
    override val toCode: String = "vars[$dest] = $x"
}
data class IterCount(val dest: Int, val iter: Int): Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = vm.access.count(iter) }
    override val toCode: String = "vars[$dest] = (double) access.count($iter)"
}
data class IterLength(val dest: Int, val iter: Int): Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = vm.access.length(iter) }
    override val toCode: String = "vars[$dest] = (double) access.length($iter)"
}
data class IterBG(val dest: Int, val iter: Int): Op() {
    override fun exec(vm: VMState) { vm.mem[dest] = vm.access.bg(iter) }
    override val toCode: String = "vars[$dest] = access.bg($iter)"
}

class VMState(staticVars: DoubleArray) {
    var mem = DoubleArray(64)
    lateinit var access : IteratorAccess
    var done = false
    var output = -Double.MAX_VALUE

    fun init(access: IteratorAccess) {
        this.access = access
        this.done = false
        this.output = -Double.MAX_VALUE
    }
    init {
        staticVars.forEachIndexed { i, x -> mem[i] = x }
    }
    fun execDynamic(prog: List<Op>, access: IteratorAccess): Double {
        init(access)
        prog.forEach { it.exec(this) }
        return output
    }
    fun exec(prog: List<Op>, access: IteratorAccess): Double {
        val vars = mem
        var pc = 0
        while(pc < prog.size) {
            val op: Op = prog[pc++]
            when (op) {
                is ReturnOp -> return vars[op.src]
                is AddOp -> vars[op.dest] = vars[op.lhs] + vars[op.rhs]
                is MultOp -> vars[op.dest] = vars[op.lhs] * vars[op.rhs]
                is DivOp -> vars[op.dest] = vars[op.lhs] / vars[op.rhs]
                is LogDivOp -> vars[op.dest] = Math.log(vars[op.lhs] / vars[op.rhs])
                is LoadConst -> vars[op.dest] = op.x
                is LogOp -> vars[op.dest] = Math.log(vars[op.src])
                is IterCount -> vars[op.dest] = access.count(op.iter).toDouble()
                is IterLength -> vars[op.dest] = access.length(op.iter).toDouble()
                is IterBG -> vars[op.dest] = access.bg(op.iter)
            }
        }
        println("Error, malformed program.")
        return -Double.MAX_VALUE;
    }
}

sealed class ExprTree() {
    abstract fun eval(doc: Int): Double
}
data class MeanET(val children: List<ExprTree>): ExprTree() {
    override fun eval(doc: Int): Double = children.sumByDouble { it.eval(doc) }
}
data class DirET(val access: IteratorAccess, val iter: Int, val mu: Double) : ExprTree() {
    val bg = access.bg(iter)
    override fun eval(doc: Int): Double {
        val count = access.count(iter) + bg
        val length = access.length(iter) + mu
        return Math.log(count / length)
    }
}

data class ManualInlineDirichlet(val access: IteratorAccess, val mu: Double) {
    val bgs = (0 until numTerms).map { access.bg(it) }.toDoubleArray()
    fun eval(): Double {
        var sum = 0.0
        (0 until numTerms).forEach { term ->
            val top = access.count(term) + bgs[term]
            val bot = access.length(term) + mu
            sum += Math.log(top / bot)
        }
        return sum / numTerms.toDouble()
    }
}

class IteratorAccess(rand: Random, n: Int) {
    private val counts = rand.ints(0, 20).asSequence().take(n).toList()
    private val lengths = rand.ints(30,100).asSequence().take(n).toList()
    private val bgs = rand.ints(200,2000).mapToDouble { it/1e10 }.asSequence().take(n).toList()

    init {
        println(counts)
        println(lengths)
        println(bgs)
    }

    fun count(i: Int): Double {
        return counts[i].toDouble()
    }
    fun length(i: Int): Double {
        return lengths[i].toDouble()
    }
    fun bg(i: Int): Double = bgs[i]
}

val numTerms = 30
fun main(args: Array<String>) {
    val mu = 1500.0

    val dirichletProgram = ArrayList<Op>().apply {
        // clear any previous runs of program.
        add(LoadConst(1, 0.0))
        val off = 3+numTerms
        (0 until numTerms).forEach { term ->
            add(IterCount(off, term))
            add(AddOp(off,3 + term, off))
            add(IterLength(off+1, term))
            add(AddOp(off+1,0, off+1))

            add(LogDivOp(off,off+1,off))
            //add(LogOp(off))
            add(AddOp(off, 1, 1))
            //add(MultOp(off, 1, 1))
        }
        // m[1] /= m[2] (sum / nTerms)
        //add(DivOp(1,2,1))
        add(ReturnOp(1))
    }
    println(dirichletProgram.joinToString(prefix="\t", separator=";\n\t") { it.toCode })
    val access = IteratorAccess(Random(), numTerms)

    val consts = listOf(mu, 0.0, numTerms.toDouble())
    val termBGs = (0 until numTerms).map { access.bg(it) }
    val staticVars = (consts + termBGs).toDoubleArray()

    val stats = StreamingStats()
    val vm_stats = StreamingStats()
    val expr_stats = StreamingStats()
    val total = 15_000_000
    val msg = CountingDebouncer(total.toLong())
    (0 until total).forEach { docNo ->
        val vm = VMState(staticVars)
        val treeProg = MeanET((0 until numTerms).map { DirET(access, it, mu) })
        val compiled = ManualInlineDirichlet(access, mu)

        // TODO, put docNo into the VM
        val time = System.nanoTime()
        // VM... {mean=1.880797492200052E-6, variance=4.2239663644248974E-11, stddev=6.499204847075446E-6, max=0.012018822, min=1.519E-6, total=28.211962384168316, count=1.5E7}
        // execDynamic is 2x as slow as exec
        // VM... {mean=3.4690132921333955E-6, variance=3.138518663244094E-11, stddev=5.60224835511966E-6, max=0.008321525, min=2.83E-6, total=52.03519938215575, count=1.5E7}
        //val vmScore = vm.exec(dirichletProgram, access)

        // Compilation appears to close the gap, but flattened appears not-as-good as other setup. Maybe only for more complex programs?
        // VM... {mean=1.2791449622001782E-6, variance=2.647106701126952E-10, stddev=1.6269931472280245E-5, max=0.02598772, min=1.137E-6, total=19.187174433356162, count=1.5E7}
        val vmScore = compiled.eval()
        val end_time = System.nanoTime()

        val t0 = System.nanoTime()
        // Expr... {mean=1.2013165588667326E-6, variance=6.510513065204406E-12, stddev=2.5515707055075716E-6, max=0.005129041, min=1.013E-6, total=18.01974838262201, count=1.5E7}
        val treeScore = treeProg.eval(docNo)
        val t1 = System.nanoTime()

        expr_stats.push((t1-t0) / 1e9)
        vm_stats.push((end_time-time) / 1e9)

        stats.push(vmScore)

        //if (vmScore != treeScore) {
        //}
        msg.incr()?.let { upd ->
            if (vmScore != treeScore) {
                println("Score mismatch? $vmScore $treeScore")
            }
            println(upd)
        }
    }
    System.out.println("Expr... ${expr_stats}")
    System.out.println("VM... ${vm_stats}")
    System.out.println(stats)
}
