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

class client{
       public:
        int clientSockfd;
        client(): clientSockfd(-1){}
        explicit client(int fd): clientSockfd(fd){}

        client(client &&clref) noexcept{
            if(this != &clref){
                clientSockfd = std::move(clref.clientSockfd);
                clref.clientSockfd = -1;
            }
        }
        client& operator=(client &&clref) noexcept{
            if(this != &clref){
                if(clientSockfd >= 0){
                    close(clientSockfd);
                }
                clientSockfd = std::move(clref.clientSockfd);
                clref.clientSockfd = -1;
            }
            return *this;
        }
        //Copy constructor is invalidated
        client(const client&) = delete;
        //Copy assignment operator is invalidated
        client& operator=(const client&) = delete;

        ~client(){
            if(clientSockfd >= 0){
                close(clientSockfd);
                clientSockfd = -1;
            }
        }
};

class server{
       public:
        int servSockfd;//Server socket fd
        int signalfd;//signal fd
        int epfd;//epoll fd
        std::vector<client> clientSockfd;
        server(){
            servSockfd = -1;
            signalfd = -1;
            epfd = -1;

        }
        ~server(){
            if(servSockfd>=0){
                close(servSockfd);
                servSockfd = -1;
            }
            if(signalfd>=0){
                close(signalfd);
                signalfd = -1;
            }
            if(epfd>=0){
                close(epfd);
                epfd = -1;
            }
        }
};

int main()
{
    server srv;
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

    srv.signalfd = signalFd;

    //1. Create server socket
    int servSockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(servSockfd < 0){
        std::cerr<<"socket creation failed!\n";
        return 1;
    }

    std::cout<<"Socket call succesfull\n";

    srv.servSockfd = servSockfd;

    //servSockfd is marked 'nonblocking' so that 'accept' doesn't block
    //the call flow if no pending connections are there in queue. It
    //fails and returns immediately with 'EAGAIN' or 'EWOULDBLOCK'
    int flags = fcntl(srv.servSockfd, F_GETFL);
    if (flags == -1) {
        int err=errno;
        std::cerr<<"fcntl(F_GETFL) failed with error:("<<err<<") :"
                 <<std::strerror(err)<<std::endl;
        return 1;
    }
    if(fcntl(srv.servSockfd, F_SETFL, flags | O_NONBLOCK) == -1){
        int err=errno;
        std::cerr<<"fcntl(F_SETFL) failed with error:("<<err<<") :"
                 <<std::strerror(err)<<std::endl;
        return 1;
    }

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
        return 1;
    }
    std::cout<<"bind call successfull\n";
    //4. listen for connection request
    if(listen(servSockfd, 5) < 0){
        std::cerr<<"listen call failed\n";
        return 1;
    }
    std::cout<<"listen call successfull\n";
    //5. Create new epoll instance (fd)
    int epfd = epoll_create1(0);
    if(epfd == -1){
        std::cerr<<"epoll_create1 failed\n";
        return 1;
    }

    std::cout<<"epoll_create1 successfull\n";

    srv.epfd = epfd;

    epoll_event signalEvent{};
    signalEvent.events = EPOLLIN;
    signalEvent.data.fd = srv.signalfd;

    if (epoll_ctl(srv.epfd, EPOLL_CTL_ADD, srv.signalfd, &signalEvent) == -1) {
        perror("epoll_ctl signalFd");
        return 1;
    }

    epoll_event ev{}, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = srv.servSockfd;

    //6. Add entry for servSockfd to interest list of epoll instance(epfd).
    // we ask kernel to look for any connection request for servSockfd
    if(epoll_ctl(srv.epfd, EPOLL_CTL_ADD, srv.servSockfd, &ev) < 0){
        std::cerr<<"epoll_ctl failed for adding servSockfd\n";
        return 1;
    }
    std::cout<<"epoll_ctl call successfull\n";

    while(running){
        //7. Keep waiting indefinitely for any event for file descriptors
        // present in epoll instance (epfd) interest list. Return such events
        // in buffer 'events'. In present case, only signalfd & servSockfd file 
        // descriptors are present in epfd interest list. Hence waiting will be done for
        // any events for these fds. Returns number of fds ready for requested I/O
        // operation.
        int nfds = epoll_wait(srv.epfd, events, MAX_EVENTS, -1);
        if(nfds < 0){
            std::cerr<<"epoll_wait failed\n";
            if(errno == EINTR){
                continue; // since SIGINT, SIGTERM are blocked this is not normal shutdown path
            }
            return 1;
        }
        std::cout<<"epoll_wait returned, nfds:"<<nfds<<"\n";

        for(int i=0; i<nfds; i++){
            if (events[i].data.fd == srv.signalfd) {
                signalfd_siginfo info{};
                ssize_t n;

                do {
                    n = read(srv.signalfd, &info, sizeof(info));
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
            else if(events[i].data.fd == srv.servSockfd){
                std::cout<<"connection request for server\n";
                //The drain loop 1: it accepts all incoming connection requests
                //till 'accept' reports EAGAIN/EWOULDBLOCK
                while(true){
                    int clientSockfd = accept(srv.servSockfd, nullptr, nullptr);
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

                    client accepted(clientSockfd);
                    std::cout<<"accept call successfull for server connection request\n";

                    //will create a new client and pass clientSockfd to its constructor
                    srv.clientSockfd.emplace_back(std::move(accepted));

                    //clientSockfd is marked 'nonblocking' so that 'read' doesn't block
                    //the call flow if no more data is there in queue. It
                    //fails and returns immediately with 'EAGAIN' or 'EWOULDBLOCK'
                    int clientflags = fcntl(clientSockfd, F_GETFL);
                    if(clientflags == -1){
                        int err=errno;
                        std::cerr<<"fcntl(F_GETFL) clientSockfd failed with error:("<<err<<") :"
                                <<std::strerror(err)<<std::endl;
                        srv.clientSockfd.pop_back();//remove the appended client
                        continue;
                    }
                    if(fcntl(clientSockfd, F_SETFL, clientflags | O_NONBLOCK) == -1){
                        int err=errno;
                        std::cerr<<"fcntl(F_SETFL) clientSockfd failed with error:("<<err<<") :"
                                <<std::strerror(err)<<std::endl;
                        srv.clientSockfd.pop_back();//remove the appended client
                        continue;
                    }


                    epoll_event clientEv{};
                    //Client Sockets events are configured as 'edge-triggered' (EPOLLET)
                    clientEv.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
                    clientEv.data.fd = clientSockfd;
                    if(epoll_ctl(srv.epfd, EPOLL_CTL_ADD, clientSockfd, &clientEv) < 0){
                        perror("epoll_ctl failed for adding clientSockfd");
                        srv.clientSockfd.pop_back();//remove the appended client
                        continue;
                    }
                }
            }else{
                char buf[512];
                const uint32_t revents = events[i].events;
                const bool terminalEvent =
                    revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP);

                if (revents & EPOLLERR) {
                    int socketError = 0;
                    socklen_t len = sizeof(socketError);

                    if (getsockopt(events[i].data.fd, SOL_SOCKET, SO_ERROR,
                                &socketError, &len) == -1) {
                        perror("getsockopt(SO_ERROR)");
                    } else if (socketError != 0) {
                        std::cerr << "Socket error on fd " << events[i].data.fd
                                << ": " << std::strerror(socketError) << '\n';
                    }
                }

                bool closeClient = false;

                for (;;) {
                    ssize_t n = read(events[i].data.fd, buf, sizeof(buf));

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
                    if (epoll_ctl(srv.epfd, EPOLL_CTL_DEL, events[i].data.fd, nullptr) == -1) {
                        perror("epoll_ctl(DEL)");
                    }
                    //Delete this specific client from vector
                    for(auto it=srv.clientSockfd.begin();it!=srv.clientSockfd.end();){
                        if(it->clientSockfd == events[i].data.fd){
                            it = srv.clientSockfd.erase(it);
                            break;
                        }
                        ++it;
                    }
                }
            }
        }//For loop
    }//While loop

    if(!running){
        std::cerr<<"Server is stopping, closing epoll fd, server socket and client sockets...\n";
    }

    return 0;
}
