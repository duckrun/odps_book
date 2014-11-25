referer<-read.csv('d:/book/data/coolshell_output/adm_refer_info.csv', header=FALSE, sep="\001")
colnames(referer)=c("site","cnt")
count <- referer$cnt
sites <- referer$site
percentage <- round(count/sum(count)*100) 
label <- paste(sites, " ", percentage, "%", sep="")
pie(count, labels=label, col=rainbow(length(label)),main="网站来源")

