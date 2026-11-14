#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <ucontext.h>
#include <pthread.h>
#include <unistd.h>
#include <link.h>
#include <stdatomic.h>

#define MAXS (1<<22)
static unsigned long samples[MAXS];
static atomic_ulong nsamp;
static volatile int stopped;

static void handler(int sig, siginfo_t *si, void *uc_) {
    (void)sig; (void)si;
    if (stopped) return;
    ucontext_t *uc = uc_;
    unsigned long n = atomic_fetch_add(&nsamp, 1);
    if (n < MAXS) samples[n] = (unsigned long)uc->uc_mcontext.gregs[REG_RIP];
}

static void *starter(void *a) {
    (void)a;
    const char *d = getenv("PROF_DELAY_MS");
    long ms = d ? atol(d) : 0;
    usleep(ms * 1000);
    const char *p = getenv("PROF_PERIOD_US");
    long us = p ? atol(p) : 200;
    struct itimerval it = {{0, us}, {0, us}};
    setitimer(ITIMER_PROF, &it, NULL);
    return NULL;
}

__attribute__((constructor)) static void init(void) {
    if (!getenv("PROF_OUT")) return;
    struct sigaction sa; memset(&sa, 0, sizeof sa);
    sa.sa_sigaction = handler; sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sigaction(SIGPROF, &sa, NULL);
    pthread_t t; pthread_create(&t, NULL, starter, NULL); pthread_detach(t);
}

__attribute__((destructor)) static void fini(void) {
    const char *out = getenv("PROF_OUT");
    if (!out) return;
    stopped = 1;
    struct itimerval z = {{0,0},{0,0}}; setitimer(ITIMER_PROF, &z, NULL);
    FILE *f = fopen(out, "w"); if (!f) return;
    FILE *m = fopen("/proc/self/maps", "r"); char line[1024];
    while (m && fgets(line, sizeof line, m)) fprintf(f, "# %s", line);
    if (m) fclose(m);
    unsigned long n = atomic_load(&nsamp); if (n > MAXS) n = MAXS;
    for (unsigned long i = 0; i < n; i++) fprintf(f, "%lx\n", samples[i]);
    fclose(f);
}
