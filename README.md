# Turkish Market Microstructure Analysis

### Notice

This is an unfinished project

## Introduction

As part of the Master Thesis, I try to understand Market Impact on BIST stocks. By using both order book and transaction book data. To do that first I created a structure script to run in just 1 month needed manipulation. In further steps I'll run this structure on all owned timeline. from 2016 to 2018. Also we're planing to obtain to date orderbook data.

Another object of the project is test Kyle et al [^readme-1]

[^readme-1]: Kyle, Albert S., and Anna A. Obizhaeva. "Market microstructure invariance: Empirical hypotheses." Econometrica 84.4 (2016): 1345-1404.]

## Data

BIST prepares the orderbook and transaction book data through BISTECH. Relevant Documentation about file pattern formats for the data files produced by BISTECH can be find through this [link](https://datastore.borsaistanbul.com/assets/files/DataStore_Veri_Bildirim_ve_Kabul_Formatları.pdf){.uri}

Important part is the transition from old system the BISTECH occurred at 30.11.2015. To use previous data another script to manipulate to prepare the data the make same analysis.

## Method

First we get the snapshot of the order-book in 5 mins intervals. Then we send artificial orders to measure the market impact. The artificial orders size determined by previous 20 days average daily volume. And we send %0.5 %1 %2 of that daily volume for each interval for each stock. that market impact gets the idea from transient market impact methodology. This methodology and its extensions were examined in detail in review paper by Gatheral et al.[^readme-2]

[^readme-2]: Gatheral, Jim, and Alexander Schied. "Dynamical models of market impact and algorithms for order execution." Handbook on Systemic Risk, Jean-Pierre Fouque, Joseph A. Langsam, eds (2013): 579-599.

Secondly we try to test mentioned hypothesis from Kyle by using available data. Although we don't have broker data but data from the exchange. We can't exactly test it but plan a slightly different replication. At the end we can't find proposed invariance in BIST.

## Final Remarks

The details of the project can be found from translation of project report which is uploaded the repo wrtitten by Kuzubas and me.
