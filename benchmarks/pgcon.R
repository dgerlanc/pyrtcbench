suppressMessages({
  library(data.table)
  library(ggplot2)
  library(bootES)
})

options(scipen=100)

infile = 'benchmark.csv'

if (!file.exists(infile)) {
  require(RPostgreSQL)
  conn = dbConnect(PostgreSQL(), 
                   user='postgres', password='postgres', dbname='postgres',
                   host='localhost')
  sql = 'select
            insert_method, logging, n_users, n_initial_stats, n_inserted_stats,
            duration
        from
            bench.benchmarks'
  dat = as.data.table(dbGetQuery(conn, sql))
  dbDisconnect(conn)
  fwrite(dat, file=infile)
} else {
  dat = fread('benchmark.csv')
}

# copy-logged vs insert-logged
log_levels = c('logged', 'unlogged')
insert_method = c('copy', 'insert')
n_users = c(1000, 10000, 50000, 100000, 500000)

conditions = dat[, unique(paste(insert_method, logging, sprintf('%d', n_users), sep="-"))]

# ------------
# Create plots
# ------------

plot_labs = labs(x='Logging Type', y='Duration (milliseconds)', color='Insert Method')
plot_png <- function(filename) {
  png(filename, height=6, width=10, res=300, units='in', antialias="subpixel")
}

plot_png('n_users-1k.png')
ggplot(dat[n_users == 1000 & duration < 0.22, ], 
       aes(x=logging, y=I(1000 * duration), color=insert_method)) + geom_boxplot() + 
  theme_bw() + plot_labs
dev.off()

plot_png('n_users-10k.png')
ggplot(dat[n_users == 10000, ], 
       aes(x=logging, y=I(1000 * duration), color=insert_method)) +
  geom_boxplot() + theme_bw() + plot_labs
dev.off()

plot_png('n_users-50k.png') 
ggplot(dat[n_users == 50000 & duration < 10], 
       aes(x=logging, y=duration, color=insert_method)) +
  geom_boxplot() + theme_bw() + 
  labs(x='Logging Type', y='Duration (seconds)', color='Insert Method')
dev.off()

plot_png('n_users-100k.png') 
ggplot(dat[n_users == 100000], aes(x=logging, y=duration, color=insert_method)) +
  geom_boxplot() + theme_bw() + 
  labs(x='Logging Type', y='Duration (seconds)', color='Insert Method')
dev.off()

plot_png('n_users-500k.png') 
ggplot(dat[n_users == 500000], aes(x=logging, y=duration, color=insert_method)) +
  geom_boxplot() + theme_bw() +
  labs(x='Logging Type', y='Duration (seconds)', color='Insert Method')
dev.off()

# ----------------------------------
# Run bootstrap statistical analyses
# ----------------------------------

set.seed(2701) # make reproducible
res = list()
mmat = expand.grid(logging=log_levels, n_users=n_users, stringsAsFactors=F)

for (idx in seq_len(nrow(mmat))) {
    n_user = mmat$n_users[idx]
    log_level = mmat$logging[idx]
    d = dat[dat$n_users == n_user & log_level == logging, ]
    analysis = bootES(
      d, data.col='duration', group.col='insert_method', 
      effect.type='unstandardized', contrast=c('insert'=1, 'copy'=-1))
    key = paste(n_user, log_level, sep="-")
    res[[key]] = analysis
}

# Extract bca CIs and estimates
cis = t(sapply(res, function(x) {
  ci = boot.ci(x, type='bca')
  c(ci$bca[4], ci$t0, ci$bca[5])
}))

# Add names and convert `logging` and `n_users` to factor
ci_dat = cbind(mmat, 1000 * cis) # bind and convert CIs to milliseconds
colnames(ci_dat)[3:5] = c('lower', 'est', 'upper')
ci_dat = data.table(ci_dat)
ci_dat[, logging:=as.factor(logging)]
ci_dat[, n_users:=as.factor(n_users)]

# Plot results separately
for (n in n_users) {
  nchr = sprintf('%d', n)
  fout = sprintf('bca-cis-n%dk.png', n %/% 1000)
  png(fout, height=6, width=10, res=300, units='in', antialias="subpixel")
  p = ggplot(ci_dat[n_users == nchr], aes(logging, est, color=logging)) + 
    geom_errorbar(aes(ymin=lower, ymax=upper), show.legend=F) + 
    geom_point(aes(y=est), show.legend=F) +
    geom_hline(yintercept=0, alpha=0.5) +
    labs(x='', y='INSERT - COPY\nDuration (milliseconds)', color='') +
    theme_bw() + theme(axis.title.y = element_text(angle=0, vjust=0.55))
  print(p)
  dev.off()  
}

# 

p1 = ggplot(ci_dat, aes(logging, est, color=logging)) + 
  facet_wrap(~ n_users, scales='free', labeller=label_both, ncol=3) 
p1a = p1 + geom_errorbar(aes(ymin=lower, ymax=upper)) + geom_point(aes(y=est)) +
  geom_hline(yintercept=0, alpha=0.5) +
  labs(x='', y='INSERT - COPY Duration (milliseconds)', color='')

png('bca-cis.png', height=6, width=10, res=300, units='in', antialias="subpixel")
print(p1a)
dev.off()
