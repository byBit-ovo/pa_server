#include "Uring.h"

pa::Uring::Uring(int queue_size):
		m_ring(std::make_unique<io_uring>()),
		m_qSize(queue_size)
{
	int ret=io_uring_queue_init(64, m_ring.get(), 0);
	if(ret!=0){
		perror("init queue error");
        exit(1);
	}
}

