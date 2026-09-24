#include <pthread.h>
#include <signal.h>
#include <unistd.h>

static void *worker(void *argument) {
    (void)argument;
    sleep(30);
    return NULL;
}

int main(void) {
    pthread_t thread;
    signal(SIGTERM, SIG_IGN);
    if (pthread_create(&thread, NULL, worker, NULL) != 0) {
        return 1;
    }
    pthread_exit(NULL);
}
