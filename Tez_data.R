library(tidyverse)
library(arrow)
setwd("C:/Users/Administrator/Desktop")


colnam <- c(
  "Date",
  "OrderNo",
  "OrderEntryTime",
  "OrderUpdateTime",
  "Ticker",
  "Buy_Sell",
  "OrderType",
  "ExchangeOrderType",
  "OrderCategory",
  "OrderDurationID",
  "OrderStatus",
  "ChangeReason",
  "Quantity",
  "RemainingQuantity",
  "SeenQuantity",
  "Price",
  "AgencyCodeFlag",
  "Session",
  "BestBidPrice",
  "BestAskPrice",
  "PrevOrderNo",
  "PartnershipAgreementNumber",
  "TransactionQuantity",
  "GiveupFlag",
  "UpdateNo",
  "UpdateTime"
)


tic <- Sys.time()
dat1 <- read_delim(file="PP_GUNICIEMIR.M.201704.csv",delim=";",col_names = colnam,
                 col_types = "DcTTcfiiicnnnnndccddccdciT",locale = locale(decimal=".",grouping_mark = ","))

tac <- Sys.time()
print(tac-tic) #7 mins


#Filtering
## Stock versus oth
dat2 <- dat1 %>% filter(str_detect(Ticker, "\\.E$")) %>%
  filter(OrderType == 1)
tac <- Sys.time()
print(tac-tic) #2 mins

rm(dat1)

#savedat2 as arrowdataset
write_dataset(dat2 %>% group_by(Date,Ticker),path="D:/Sami_Tez_Data/Raw")
dates <- dat2 %>% distinct(Date) %>% arrange %>% pull %>% as.character()
tickers <- dat2 %>% distinct(Ticker) %>% pull
rm(dat2)

#Alternative dates&tickers
dates <- list.dirs("D:/Sami_Tez_Data/Raw",recursive = F) %>% strsplit(.,"=") %>% sapply(., "[[", 2)
tickers <- list.dirs("D:/Sami_Tez_Data/Raw") %>% str_detect("Ticker=") %>%
  list.dirs("D:/Sami_Tez_Data/Raw")[.] %>% str_extract(.,"(?<=Ticker=)[^/]+") %>% unique()


#create POVtable
colnam2 <- c(
  "Date",
  "Ticker",
  "PartnershipAgreementNumber",
  "MemberContractNo",
  "Buy_Sell",
  "Quantity",
  "Price",
  "Volume",
  "OrderNo",
  "TransactionTime",
  "DealSource",
  "Session",
  "TradeType",
  "SettlementDate",
  "Active_Passive",
  "PartyTransactionNo",
  "PartyAgreementNo",
  "TransactionUpdateTime",
  "GiveupFlag",
  "UpdateNo",
  "UpdateTime"
)

POVtable <- read_delim(file="PP_GUNICIISLEM.M.201703.csv",delim=";",col_names=colnam2,
           col_types= "DcccfdddcticccfnnTciT",locale = locale(decimal=".",grouping_mark = ",")) %>%
  filter(str_detect(Ticker, "\\.E$")) %>% select(Date,Ticker,Buy_Sell,Quantity) %>%
  group_by(Date,Ticker) %>% summarise(Quant=sum(Quantity)) %>% ungroup
POVtable2 <- read_delim(file="PP_GUNICIISLEM.M.201704.csv",delim=";",col_names=colnam2,
                                col_types= "DcccfdddcticccfnnTciT",locale = locale(decimal=".",grouping_mark = ",")) %>%
  filter(str_detect(Ticker, "\\.E$")) %>% select(Date,Ticker,Buy_Sell,Quantity) %>%
  group_by(Date,Ticker) %>% summarise(Quant=sum(Quantity)) %>% ungroup
POVtable <- rbind(POVtable,POVtable2)
rm(POVtable2)
library(RcppRoll)
POVtable <- POVtable %>% group_by(Ticker) %>% mutate(Vol20=roll_meanr(Quant,20)) %>% ungroup %>% na.omit()

#Create POV table
getPOV <- function(ticker,date,percent){
  vol <- POVtable %>% filter(Date==date,Ticker==ticker) %>% pull(Vol20)
  POV <- vol*(percent/100)
  return(POV)
}
getMIprice <- function(buy_sell,quan){
  if (buy_sell == "S") {
    predat_a %>% arrange(desc(Price)) %>% 
      filter(sumVol > quan) %>% slice(1) %>% 
      select(Price) %>% pull -> MIprice
  } else if(buy_sell=="A"){
    predat_s  %>% arrange(Price) %>% 
      filter(sumVol > quan) %>% slice(1) %>% 
      select(Price) %>% pull -> MIprice
  }
  
  return(MIprice)
}

# main Operation to get Market Impact
tic2 <-Sys.time()


starttime <- "10:00:00"
endtime <- "18:10:00"
interval_seconds=300
res <- tibble(Ticker=character(),Date=as.Date(character()),Interval=as.POSIXct(character()),
              Buy_Sell=factor(),POV=numeric(),MIprice=numeric(),MP=numeric(),
              MI=numeric(),BA=numeric())
c = 1
interval_vector <- seq(from=as.POSIXct(starttime,format="%H:%M:%S",tz="UTC"),
                       to=as.POSIXct(endtime,format="%H:%M:%S",tz="UTC"),
                       by=interval_seconds)
n_iter <- length(interval_vector)*length(dates)*length(tickers)
rm(interval_vector)
library(foreach)
library(doParallel)
library(parallel)
n.cores <- parallel::detectCores() -4
my.cluster <- parallel::makeCluster(n.cores,type = "PSOCK")
doParallel::registerDoParallel(cl=my.cluster)
tac2 <- Sys.time()
print(tac2-tic2)

result <- foreach(d=dates,.combine = "rbind") %:%
  foreach(t=tickers,.combine = "rbind")%:%
  foreach(int=as.character(seq(from=as.POSIXct(paste0(d," ",starttime),tz="UTC"),
                               to=as.POSIXct(paste0(d," ",endtime),tz="UTC"),
                               by=interval_seconds)),
          .combine = "rbind",
          .packages = c("dplyr","arrow")) %dopar%
  {
   predat <-  read_parquet(paste0("D:/Sami_Tez_Data/Raw/","Date=",d,"/Ticker=",t,"/part-0.parquet")) %>%
     filter(OrderUpdateTime <= as.POSIXct(int,tz="UTC")) %>%
     group_by(OrderNo) %>%  filter(UpdateNo==max(UpdateNo)) %>% ungroup %>%
     filter(OrderStatus==1) %>%
     mutate(Interval=int) 
   predat_s <- predat %>% filter(Buy_Sell=="S") %>% group_by(Price) %>%
     summarise(sumQ=sum(RemainingQuantity)) %>% ungroup %>%  arrange(Price) %>%
     add_row(Price=max(.$Price),sumQ=Inf) %>% 
     mutate(sumVol=cumsum(sumQ))
   predat_a <- predat %>% filter(Buy_Sell=="A") %>% group_by(Price) %>%
     summarise(sumQ=sum(RemainingQuantity)) %>% ungroup %>% arrange(desc(Price)) %>%
     add_row(Price=min(.$Price),sumQ=Inf) %>% 
     mutate(sumVol=cumsum(sumQ))
   MPBA <- predat %>% filter(BestBidPrice!=0,BestAskPrice!=0) %>% arrange(desc(OrderUpdateTime)) %>% slice(1) %>%
     mutate(MP=(BestBidPrice+BestAskPrice)/2,BA=BestAskPrice-BestBidPrice) %>%select(MP,BA) %>%  as.list()
   rm(predat)
   dat <- tibble(Ticker=t,Date=d,Interval=int,Buy_Sell=c(rep("A",3),rep("S",3)),
                 POV=rep(c(0.5,1,2),2),MP=MPBA$MP[1],BA=MPBA$BA[1])
   dat %>% rowwise %>% mutate(TQuantity = getPOV(Ticker,Date,POV)) %>% 
     mutate(MIprice = getMIprice(Buy_Sell,TQuantity)) %>% ungroup() %>% 
     mutate(MI = MIprice-MP) %>% select(Ticker,Date,Interval,Buy_Sell,POV,MIprice,MP,BA) ->dat
   
   if (dir.exists(paste0("D:/Sami_Tez_Data/Post/","Date=",d,"/Ticker=",t,
                         "/Interval=",gsub(x=strsplit(int," ")[[1]][2],pattern=":",replacement="_")))) {
     unlink(recursive = T,x = paste0("D:/Sami_Tez_Data/Post/","Date=",d,"/Ticker=",t,
                   "/Interval=",gsub(x=strsplit(int," ")[[1]][2],pattern=":",replacement="_")))
   }
   dir.create(paste0("D:/Sami_Tez_Data/Post/","Date=",d,"/Ticker=",t,
                     "/Interval=",gsub(x=strsplit(int," ")[[1]][2],pattern=":",replacement="_")),recursive = T)
   write_parquet(dat,paste0("D:/Sami_Tez_Data/Post/","Date=",d,"/Ticker=",t,
                            "/Interval=",strsplit(int," ")[[1]][2] %>% gsub(x=.,pattern=":",replacement="_"),
                            "/part-0.parquet"))
   rm(dat,predat_s,predat_a,MPBA)
   1
  }


stopCluster(my.cluster)
tac2 <- Sys.time()
print(tac2-tic2)

#23.16-18.37
##
open_dataset("D:/Sami_Tez_Data/Post") %>% filter(Ticker=="BIMAS.E") %>% collect()-> dat
open_dataset("D:/Sami_Tez_Data/Post") %>%
  mutate(Impact_step=(MIprice-MP)/BA) %>%
  collect() -> dat

dat <-dat %>% mutate(deneme=paste0(Date," ",Interval)) %>% 
  mutate(deneme=gsub("_",":",deneme)) %>% mutate(Interval_time=as.POSIXct(deneme)) %>% select(-deneme)

#Plots to see market Impact
dat  %>%
  filter(Ticker=="ACSEL.E",Date=="2017-04-03",Buy_Sell=="A",POV==1) %>%
    ggplot(aes(x=Interval_time,y=Impact_step)) + 
    geom_point()

dat %>% filter(Ticker=="BIMAS.E") %>% group_by(Interval,Buy_Sell,POV) %>%
  summarise(Impact_step_mean=mean(Impact_step)) %>% ungroup %>%
  mutate(Interval_time=as.POSIXct(Interval,format = "%H_%M_%S")) %>%
  mutate(POV=as.factor(POV),Impact_step_mean=ifelse(Buy_Sell=="S",-Impact_step_mean,Impact_step_mean)) %>% 
  group_by(Buy_Sell) %>% 
    ggplot(aes(x=Interval_time,y=Impact_step_mean,color=POV)) + facet_wrap("Buy_Sell")+
    geom_point() +labs(title="Market Impact Throughout the Day",
                       subtitle="BIMAS.E",x="Timestamp",y="Market Impact")

dat %>% mutate(POV=factor(POV,levels=c(2,1,0.5),labels = c("2%","1%","0.5%"),ordered = T)) %>% filter(Impact_step<100,Impact_step>-100)%>%  group_by(Ticker,Interval,Buy_Sell,POV) %>%
  summarise(Impact_step_mean=mean(Impact_step),.groups="drop")  %>% group_by(Interval,Buy_Sell,POV) %>%
  summarise(Impact_step_mean=mean(Impact_step_mean,na.rm=T),.groups="drop") %>% 
  mutate(Interval_time=as.POSIXct(Interval,format = "%H_%M_%S")) %>%
  mutate(Impact_step_mean=ifelse(Buy_Sell=="S",-Impact_step_mean,Impact_step_mean)) %>% 
  mutate(Buy_Sell=ifelse(Buy_Sell=="A","Buy","Sell")) %>% 
  group_by(Buy_Sell) %>% 
  ggplot(aes(x=Interval_time,y=Impact_step_mean,colour=POV)) + facet_wrap("Buy_Sell")+
  geom_point() + labs(title="Market Impact Throughout the Day",
                      subtitle="Impacts represented as ticks",x="Timestamp",y="Market Impact") +
  scale_color_discrete()


#Market Impact selected Stocks
liq_max <- POVtable %>% filter(Date==as.Date("2017-03-31")) %>% slice_max(n=8,order_by = Vol20) %>% pull(Ticker)
liq_min <- POVtable %>% filter(Date==as.Date("2017-03-31")) %>% slice_min(n=6,order_by = Vol20) %>% pull(Ticker)

c("EKGYO.E","ACSEL.E","AFYON.E","ANACM.E","ALKIM.E",
"PRKME.E","RTALB.E","AVTUR.E","ALCTL.E","AVGYO.E")

dat %>% filter(Ticker %in% c("EKGYO.E","ACSEL.E","AFYON.E","ANACM.E","ALKIM.E",
                             "PRKME.E","RTALB.E","AVTUR.E","ALCTL.E","AVGYO.E")
) %>% 
  mutate(Impact_perc=(MIprice-MP)/MP) %>% 
  group_by(Ticker,Interval,Buy_Sell,POV) %>% 
  summarise(Impact_perc_mean=mean(Impact_perc,na.rm=T),.groups = "drop") %>% group_by(Ticker,Buy_Sell,POV) %>% 
  summarise(Impact_perc_mean=median(Impact_perc_mean),.groups="drop") %>% 
  filter(POV==1) %>% 
  mutate(Impact_perc_mean=ifelse(Buy_Sell=="S",-Impact_perc_mean,Impact_perc_mean)) %>% select(-POV) %>%
  pivot_wider(names_from = Buy_Sell,values_from = Impact_perc_mean) %>% arrange(A) %>% write.csv("table1.csv")
  
### Try to test  Kyle et al Invariant Market Microstructure
kyle1 <- read_delim(file="PP_GUNICIISLEM.M.201703.csv",delim=";",col_names=colnam2,
           col_types= "DcccfdddcticccfnnTciT",locale = locale(decimal=".",grouping_mark = ",")) %>%
  filter(str_detect(Ticker, "\\.E$"))
kyle2 <- read_delim(file="PP_GUNICIISLEM.M.201704.csv",delim=";",col_names=colnam2,
                    col_types= "DcccfdddcticccfnnTciT",locale = locale(decimal=".",grouping_mark = ",")) %>%
  filter(str_detect(Ticker, "\\.E$"))
kyle <- rbind(kyle1,kyle2)
kyle %>% select(Date,Ticker,Price,Quantity,TransactionTime) %>% arrange(Date,Ticker,TransactionTime) %>%
  mutate(Datetime=as.POSIXct(paste0(Date," ",TransactionTime))) -> kyle

starttime <- "10:00:00"
endtime <- "18:00:00"
interval_seconds = 300
dates <- kyle %>% distinct(Date) %>% arrange() %>% pull() %>% as.character()
ints <- c()
for(d in dates){
 int <- as.character(seq(from=as.POSIXct(paste0(d," ",starttime),tz="UTC"),
                 to=as.POSIXct(paste0(d," ",endtime),tz="UTC"),
                 by=interval_seconds))
 ints <- c(ints,int)
}

kyle <- kyle %>% mutate(Interval=cut.POSIXt(Datetime,breaks = "15 mins"))

# Interval represents start point: interval that starts at 10:55:00 -> 10:55-11:00
kyle %>% group_by(Interval,Ticker) %>%
  summarise(Quantity=sum(Quantity),Vol=sd(Price),Price=mean(Price),velocity=n(),.groups="drop") %>%
  filter(Vol>0) %>% 
  mutate(Date=as.POSIXct(as.character(Interval)),Date=as.Date(Date,tz="UTC")) %>%
  mutate(Time=gsub(x=strsplit(as.character(Interval)," ") %>% sapply(., "[[", 2),pattern=":",replacement="_")) %>% 
  group_by(Ticker,Time) %>% arrange(Date) %>% 
  mutate(Quantity_ma20=roll_meanr(Quantity,20),Vol_ma20=roll_meanr(Vol,20),velocity_ma20=roll_meanr(velocity)) %>%
  na.omit
  

kyle %>% group_by(Interval,Ticker) %>% summarise(Vol=sd(Price),.groups = "drop")
  
  
  
  summarise(vol,velocity,meanprice,quantity)




















