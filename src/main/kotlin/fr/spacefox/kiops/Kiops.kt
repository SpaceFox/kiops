package fr.spacefox.kiops

import com.google.common.math.LongMath
import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import java.util.*

const val SECTOR_SIZE = 4096L

enum class AbbrevType { BINARY, SI, NONE }
val ABBREVS = mapOf(
        AbbrevType.BINARY to listOf(
                Pair((1L shl 50), "Pi"),
                Pair((1L shl 40), "Ti"),
                Pair((1L shl 30), "Gi"),
                Pair((1L shl 20), "Mi"),
                Pair((1L shl 10), "Ki"),
                Pair(1L,          "  ")
        ),
        AbbrevType.SI to listOf(
                Pair(LongMath.pow(10, 15), "P"),
                Pair(LongMath.pow(10, 12), "T"),
                Pair(LongMath.pow(10,  9), "G"),
                Pair(LongMath.pow(10,  6), "M"),
                Pair(LongMath.pow(10,  3), "K"),
                Pair(1L,                   " ")
        ),
        AbbrevType.NONE to listOf(
                Pair(1L, "")
        )
)

object BlockData {
    var data: ByteArray = ByteArray(0)
}

fun main(args: Array<String>) {
    try {
        val kiops = Kiops(args[0], nbThreads = 8, targetTime = 5000, abbrevType = AbbrevType.SI)
        kiops.test()
    } catch (e: IOException) {
        e.printStackTrace()
    } catch (e: InterruptedException) {
        e.printStackTrace()
    }

}

class Kiops(val path: String, val nbThreads: Int = 32, val targetTime: Long = 2000, val abbrevType: AbbrevType = AbbrevType.BINARY) {

    private var mediaSize: Long = 0
    private val random = Random()

    init {
        RandomAccessFile(path, "r").use { raf -> this.mediaSize = raf.length() }
        println("$path, ${sizeFormat(mediaSize)}B, sectorsize ${SECTOR_SIZE}B, #threads $nbThreads, pattern random:")
    }

    private fun sizeFormat(value: Long): String {
        return sizeFormat(value.toDouble(), precision = 0)
    }

    private fun sizeFormat(value: Double, precision: Int = 1): String {
        var factor: Long = 1
        var suffix: String = " "
        val prefixList = ABBREVS[abbrevType] ?: listOf(Pair(1L, ""))
        for ((f, s) in prefixList) {
            if (value >= f) {
                factor = f
                suffix = s
                break
            }
        }
        return "%1$.${precision}f $suffix".format(value / factor)
    }

    fun test() {
        val iops = (nbThreads + 1).toLong()  // Initial loop
        var blockSize: Long = 512
        val threads: MutableList<KiopsThread> = mutableListOf()
        while (iops > Math.max(1, nbThreads) && blockSize < mediaSize) {
            BlockData.data = ByteArray(blockSize.toInt())
            for (i in 1..nbThreads) {
                threads.add(KiopsThread(blockSize, targetTime))
            }
            for (thread in threads) {
                thread.start()
            }
            for (thread in threads) {
                thread.join()
            }
            val sum = threads.sumBy { it.count }
            val realTime = threads.sumBy { it.realTime } / (1000.0 * nbThreads)
            threads.clear()
            System.gc()
            display(blockSize, sum / realTime)
            blockSize *= 2
        }

    }

    private fun display(blockSize: Long, iops: Double) {
        val byteOut = blockSize * iops
        val bitOut = byteOut * 8
        println("%1sB blocks: %2$.1f IO/s, %3\$sB/s (%4\$sbit/s)".format(
                sizeFormat(blockSize),
                iops,
                sizeFormat(byteOut),
                sizeFormat(bitOut)
        ))
    }

    private inner class KiopsThread(private val blockSize: Long, private val targetTime: Long) : Thread() {
        var count: Int = 0
            private set
        var realTime: Int = 0
            private set

        override fun run() {
            try {
                RandomAccessFile(path, "r").use { raf ->
                    var position: Long
                    val startTime = System.currentTimeMillis()
                    val endTime = startTime + targetTime
                    while (System.currentTimeMillis() < endTime) {
                        count++
                        position = Math.abs(random.nextLong() % (mediaSize - blockSize))
                        position = position and (SECTOR_SIZE - 1).inv()
                        raf.seek(position)
                        raf.read(BlockData.data)
                    }
                    realTime = (System.currentTimeMillis() - startTime).toInt()
                }
            } catch (e: FileNotFoundException) {
                e.printStackTrace()
            } catch (e: IOException) {
                e.printStackTrace()
            }

        }
    }
}
