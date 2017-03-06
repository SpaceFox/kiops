package fr.spacefox.kiops

import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import java.util.*

const val SECTOR_SIZE: Int = 4096

fun main(args: Array<String>) {
    try {
        val kiops = Kiops(args[0])
        kiops.test()
    } catch (e: IOException) {
        e.printStackTrace()
    } catch (e: InterruptedException) {
        e.printStackTrace()
    }

}

class Kiops(private val path: String) {

    private var mediaSize: Long = 0
    private val random = Random()
    private val nbThreads = 32

    init {
        RandomAccessFile(path, "r").use { raf -> this.mediaSize = raf.length() }
        println("$path, ${sizeFormat(mediaSize)}B, sectorsize ${SECTOR_SIZE}B, pattern random:")
    }

    private fun sizeFormat(value: Long): String {
        return sizeFormat(value.toDouble(), precision = 0)
    }

    private fun sizeFormat(value: Double, precision: Int = 2): String {
        val abbrevs = listOf(
                Pair((1L shl 50),   "Pi"),
                Pair((1L shl 40),   "Ti"),
                Pair((1L shl 30),   "Gi"),
                Pair((1L shl 20),   "Mi"),
                Pair((1L shl 10),   "Ki"),
                Pair(1L,            "  ")
        )
        var factor: Long = 1
        var suffix: String = " "
        for ((f, s) in abbrevs) {
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
        while (iops > Math.max(1, nbThreads / 4) && blockSize < mediaSize) {
            val threads: MutableList<KiopsThread> = mutableListOf()
            for (i in 1..nbThreads) {
                threads.add(KiopsThread(blockSize))
            }
            for (thread in threads) {
                thread.start()
            }
            for (thread in threads) {
                thread.join()
            }
            val sum = threads.sumBy { it.count }
            display(blockSize, sum / 2.0)
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

    private inner class KiopsThread(private val blockSize: Long) : Thread() {
        var count: Int = 0
            private set

        override fun run() {
            try {
                RandomAccessFile(path, "r").use { raf ->
                    var position: Long
                    val blockData = ByteArray(blockSize.toInt())
                    val endTime = System.currentTimeMillis() + 2000
                    while (System.currentTimeMillis() < endTime) {
                        count++
                        position = Math.abs(random.nextLong() % (mediaSize - blockSize))
                        position = position and (SECTOR_SIZE - 1).inv().toLong()
                        raf.seek(position)
                        raf.read(blockData)
                    }
                }
            } catch (e: FileNotFoundException) {
                e.printStackTrace()
            } catch (e: IOException) {
                e.printStackTrace()
            }

        }
    }
}
