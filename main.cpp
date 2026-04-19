#include "reactor/main_reactor.h"

int main() {
    MainReactor server;
    if (!server.init()) {
        return 1;
    }

    server.run();
    return 0;
}

