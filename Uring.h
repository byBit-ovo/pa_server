#include <liburing.h>
#include <memory>
#include <tuple>


namespace pa{

class Uring
{
public:
	Uring(int queue_size=64);
	
private:
	std::unique_ptr<io_uring> m_ring;
	int m_qSize;
};

}// namespace pa