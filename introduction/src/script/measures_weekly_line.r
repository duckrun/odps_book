measures<-read.csv('d:/book/data/coolshell_output/adm_user_measures_weekly.csv', header=FALSE, sep="\001")
colnames(measures)=c("day","pv","uv")
sort(measures, 'day')
days <- measures$day
pvs <- measures$pv
uvs <- measures$uv
plot(x=days,y=pvs, xlab="date", ylab="pv/uv", type="l", main="PV/UV统计", col="red",ylim=c(0,100))
lines(x=days,y=uvs, col="green")
legend("topright",legend=c("PV","UV"), col=c("red", "green"), lty=c(1:2))
box()

