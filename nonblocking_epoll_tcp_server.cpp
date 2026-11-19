#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <thread>
#include <vector>
#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstring>
#include <sys/signalfd.h>
#include <signal.h>

#define MAX_EVENTS 10

int main()
{
    bool running=true;
    sigset_t signalMask;
    if (sigemptyset(&signalMask) == -1 ||
        sigaddset(&signalMask, SIGINT) == -1 ||
        sigaddset(&signalMask, SIGTERM) == -1 ||
        sigprocmask(SIG_BLOCK, &signalMask, nullptr) == -1) {
        perror("setting signal mask");
        return 1;
    }
    int signalFd = signalfd(-1, &signalMask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signalFd == -1) {
        perror("signalfd");
        return 1;
    }

    //1. Create server socket
    int servSockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(servSockfd < 0){
        std::cerr<<"socket creation failed!\n";
        close(signalFd);
        return 1;
    }

    //servSockfd is marked 'nonblocking' so that 'accept' doesn't block
    //the call flow if no pending connections are there in queue. It
    //fails and returns immediately with 'EAGAIN' or 'EWOULDBLOCK'
    int flags = fcntl(servSockfd, F_GETFL);
    if (flags == -1) {
        int err=errno;
        std::cerr<<"fcntl(F_GETFL) failed with error:("<<err<<") :"
                 <<std::strerror(err)<<std::endl;
        close(signalFd);
        close(servSockfd);
        return 1;
    }
    if(fcntl(servSockfd, F_SETFL, flags | O_NONBLOCK) == -1){
        int err=errno;
        std::cerr<<"fcntl(F_SETFL) failed with error:("<<err<<") :"
                 <<std::strerror(err)<<std::endl;
        close(signalFd);
        close(servSockfd);
        return 1;
    }

    std::cout<<"Socket call succesfull\n";
    //2. Configure server address
    sockaddr_in servaddress{};
    servaddress.sin_family = AF_INET;
    servaddress.sin_port = htons(8080);
    servaddress.sin_addr.s_addr = INADDR_ANY;

    //3. Bind server address with server socket. OS should
    //allow the server to bind to its address and port again
    //when old connections may still be in states such as 
    //TIME_WAIT
    int opt = 1;
    if((setsockopt(servSockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)))<0){
        int err = errno;
        std::cerr<<"setsockopt failed with Error:("<<err<<"): "<<std::strerror(err)<<std::endl;
    }

    if(bind(servSockfd, (struct sockaddr *)&servaddress, 
            sizeof(servaddress)) < 0){
        std::cerr<<"bind call failed\n";
        perror("bind failed");
        close(signalFd);
        close(servSockfd);
        return 1;
    }
    std::cout<<"bind call successfull\n";
    //4. listen for connection request
    if(listen(servSockfd, 5) < 0){
        std::cerr<<"listen call failed\n";
        close(signalFd);
        close(servSockfd);
        return 1;
    }
    std::cout<<"listen call successfull\n";
    //5. Create new epoll instance (fd)
    int epfd = epoll_create1(0);
    if(epfd == -1){
        std::cerr<<"epoll_create1 failed\n";
        close(signalFd);
        close(servSockfd);
        return 1;
    }

    std::cout<<"epoll_create1 successfull\n";

    epoll_event signalEvent{};
    signalEvent.events = EPOLLIN;
    signalEvent.data.fd = signalFd;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, signalFd, &signalEvent) == -1) {
        perror("epoll_ctl signalFd");
        close(signalFd);
        close(servSockfd);
        close(epfd);
        return 1;
    }

    epoll_event ev{}, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = servSockfd;

    //6. Add entry for servSockfd to interest list of epoll instance(epfd).
    // we ask kernel to look for any connection request for servSockfd
    if(epoll_ctl(epfd, EPOLL_CTL_ADD, servSockfd, &ev) < 0){
        std::cerr<<"epoll_ctl failed for adding servSockfd\n";
        close(signalFd);
        close(epfd);
        close(servSockfd);
        return 1;
    }
    std::cout<<"epoll_ctl call successfull\n";

    std::vector<int> clientSockets;

    while(running){
        //7. Keep waiting indefinitely for any event for file descriptors
        // present in epoll instance (epfd) interest list. Return such events
        // in buffer 'events'. In present case, only signalfd & servSockfd file 
        // descriptors are present in epfd interest list. Hence waiting will be done for
        // any events for these fds. Returns number of fds ready for requested I/O
        // operation.
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if(nfds < 0){
            std::cerr<<"epoll_wait failed\n";
            if(errno == EINTR){
                continue; // since SIGINT, SIGTERM are blocked this is not normal shutdown path
            }
            close(signalFd);
            close(epfd);
            close(servSockfd);
            for (int fd : clientSockets){
                close(fd);
            }
            clientSockets.clear();
            return 1;
        }
        std::cout<<"epoll_wait returned, nfds:"<<nfds<<"\n";

        for(int i=0; i<nfds; i++){
            if (events[i].data.fd == signalFd) {
                signalfd_siginfo info{};
                ssize_t n;

                do {
                    n = read(signalFd, &info, sizeof(info));
                } while (n == -1 && errno == EINTR);

                if (n == sizeof(info)) {
                    running = false;
                    break; // Leave the event batch; the outer loop will exit.
                }

                if (n == -1 && errno == EAGAIN) {
                    continue;
                }

                perror("read signalFd");
                running = false;
                break;
            }
            // Event: connection request on server socket or data on client socket
            else if(events[i].data.fd == servSockfd){
                std::cout<<"connection request for server\n";
                //The drain loop 1: it accepts all incoming connection requests
                //till 'accept' reports EAGAIN/EWOULDBLOCK
                while(true){
                    int clientSockfd = accept(servSockfd, nullptr, nullptr);
                    if(clientSockfd < 0){
                        if(errno == EINTR){
                            continue;
                        }
                        if(errno == EAGAIN || errno == EWOULDBLOCK){
                            break; // no more pending connections
                        }
                        if(errno == EBADF){
                            std::cerr<<"Server socket fd is closed, stopping server...\n";
                            running = false;
                            break;
                        }
                        perror("accept failed");
                        break;
                    }

                    std::cout<<"accept call successfull for server connection request\n";
                    //clientSockfd is marked 'nonblocking' so that 'read' doesn't block
                    //the call flow if no more data is there in queue. It
                    //fails and returns immediately with 'EAGAIN' or 'EWOULDBLOCK'
                    int clientflags = fcntl(clientSockfd, F_GETFL);
                    if(clientflags == -1){
                        int err=errno;
                        std::cerr<<"fcntl(F_GETFL) clientSockfd failed with error:("<<err<<") :"
                                <<std::strerror(err)<<std::endl;
                        close(clientSockfd);
                        continue;
                    }
                    if(fcntl(clientSockfd, F_SETFL, clientflags | O_NONBLOCK) == -1){
                        int err=errno;
                        std::cerr<<"fcntl(F_SETFL) clientSockfd failed with error:("<<err<<") :"
                                <<std::strerror(err)<<std::endl;
                        close(clientSockfd);
                        continue;
                    }

                    clientSockets.push_back(clientSockfd);

                    epoll_event clientEv{};
                    //Client Sockets events are configured as 'edge-triggered' (EPOLLET)
                    clientEv.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
                    clientEv.data.fd = clientSockfd;
                    if(epoll_ctl(epfd, EPOLL_CTL_ADD, clientSockfd, &clientEv) < 0){
                        perror("epoll_ctl failed for adding clientSockfd");
                        close(clientSockfd);
                        clientSockets.pop_back();
                        continue;
                    }
                }
            }else{
                char buf[512];
                int clientSockfd = events[i].data.fd;
                const uint32_t revents = events[i].events;
                const bool terminalEvent =
                    revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP);

                if (revents & EPOLLERR) {
                    int socketError = 0;
                    socklen_t len = sizeof(socketError);

                    if (getsockopt(clientSockfd, SOL_SOCKET, SO_ERROR,
                                &socketError, &len) == -1) {
                        perror("getsockopt(SO_ERROR)");
                    } else if (socketError != 0) {
                        std::cerr << "Socket error on fd " << clientSockfd
                                << ": " << std::strerror(socketError) << '\n';
                    }
                }

                bool closeClient = false;

                for (;;) {
                    ssize_t n = read(clientSockfd, buf, sizeof(buf));

                    if (n > 0) {
                        std::cout << "Data read from client:"
                                << std::string(buf, n) << '\n';
                        continue; // Edge-triggered: drain available data.
                    }

                    if (n == 0) {
                        closeClient = true; // EOF: peer closed its sending side.
                        break;
                    }

                    const int err = errno; // Only inspect errno when read returned -1.
                    if (err == EINTR) {
                        continue;
                    }
                    if (err == EAGAIN || err == EWOULDBLOCK) {
                        // Ordinary EPOLLIN: drained for now. A terminal event means close.
                        closeClient = terminalEvent;
                        break;
                    }

                    errno = err;
                    perror("read");
                    closeClient = true;
                    break;
                }

                if (closeClient) {
                    if (epoll_ctl(epfd, EPOLL_CTL_DEL, clientSockfd, nullptr) == -1) {
                        perror("epoll_ctl(DEL)");
                    }
                    close(clientSockfd);
                    clientSockets.erase(
                        std::remove(clientSockets.begin(), clientSockets.end(), clientSockfd),
                        clientSockets.end());
                }
            }
        }//For loop
    }//While loop

    if(!running){
        std::cerr<<"Server is stopping, closing epoll fd, server socket and client sockets...\n";
    }
    close(epfd);
    close(servSockfd);
    for(int clientSockfd : clientSockets){
        close(clientSockfd);
    }

    return 0;
}
