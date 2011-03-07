#ifndef _SYS_RECVFILE_H
#define _SYS_RECVFILE_H	1
#include <sys/sendfile.h>
#include <unistd.h>
#include <poll.h>

static inline ssize_t
recvfile(int out_fd, int in_fd, off_t *offset, size_t count)
{
	int pipefd[2];
	ssize_t remaining = count;
	struct pollfd pfd;
	ssize_t siz;

	if (pipe(pipefd) < 0) {
		perror("pipe");
		return -1;
	}

	pfd.fd = in_fd;
	pfd.events = POLLIN;
	while (remaining > 0) {
		if (poll(&pfd, 1, -1) != 1)
			continue;

		if ((siz = splice(in_fd, NULL, pipefd[1], NULL, remaining,
						  SPLICE_F_NONBLOCK | SPLICE_F_MOVE)) < 0) {
			if (remaining < count)
				return count - remaining;
			else
				return siz;
		}
		if (!siz)
			break;
		
		if ((siz = splice(pipefd[0], NULL, out_fd, offset, siz,
						  SPLICE_F_MORE | SPLICE_F_MOVE)) < 0) {
			if (remaining < count)
				return count - remaining;
			else
				return siz;
		}
		remaining -= siz;
	}
	close(pipefd[0]);
	close(pipefd[1]);
	return count - remaining;
}

#endif
