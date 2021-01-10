package com.epam.songwordcounterborisov.service

import org.springframework.stereotype.Service

@Service
class MusicJudgeService(extractText: ExtractText, topWordsService: TopWordsService) {
    def printTopWordsOfTheBand(bandName:String, x: Int): Unit ={
     val linesRdd =extractText.readLinesFromFile(s"src/main/songs/$bandName/*")
      val list=topWordsService.topX(linesRdd, x)
      print(list)
    }
}
