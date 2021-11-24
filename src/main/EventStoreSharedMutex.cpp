#include <iostream>
#include <mutex>
#include <vector>
#include <thread>
#include <string>
#include <shared_mutex>
#include <unordered_map>

void test_0(void);
void test_1(void);
void test_2(void);
void test_3(void);
void test_4(void);
void test_5(void);
void test_6(void);

// https://en.cppreference.com/w/cpp/thread/shared_mutex
// https://stackoverflow.com/questions/2695317/write-only-reference-in-c

// using namespace std;

class Event {
	std::string type;
	long int timestamp;

public: 
		Event(std::string type,long int timestamp){
			this->type      = type;
			this->timestamp = timestamp;
		}

		Event(const Event &obj){
			this->type = obj.type;
			this->timestamp = obj.timestamp;
		}

		~Event(){
		}

		std::string Type(){
			return this->type;
		}

		long int Timestamp(){
			return this->timestamp;
		}
};

/*
 * https://en.cppreference.com/w/cpp/thread/shared_mutex
 * https://ncona.com/2019/03/read-write-mutex-with-shared_mutex/
 * https://stackoverflow.com/questions/1601943/mutex-lock-on-write-only
 * https://stackoverflow.com/questions/19915152/c11-multiple-read-and-one-write-thread-mutex
 * https://stackoverflow.com/questions/2695317/write-only-reference-in-c
 * https://www.geeksforgeeks.org/c-mutable-keyword/
 * 
 * mutex pool reference -- not used but was considered in the original design
 * https://stackoverflow.com/questions/16465633/how-can-i-use-something-like-stdvectorstdmutex
 *
 * I considered writing a threadpool but have up after seeing the following link:
 * https://ncona.com/2019/05/using-thread-pools-in-cpp/
 */

// https://www.geeksforgeeks.org/unordered_multimap-and-its-application/
// https://en.cppreference.com/w/cpp/container/unordered_multimap/insert

// unordered_multimap<string, int>::iterator

class EventStore {
private: 
	std::unordered_multimap<std::string, int> event_mmap; // Simplest data structure to this problem, the alternative 
	                                                      // would have been something like
	                                                      // std::unordered_map< std::string, std::vector<int> > event_map;
	mutable std::shared_mutex sh_mutex_;

public:
	void insert(Event in_event){
		std::unique_lock<std::shared_mutex> lock(sh_mutex_);
		event_mmap.insert({ in_event.Type(), in_event.Timestamp() });
	}

	void removeAll(std::string ev_type){
		std::unique_lock<std::shared_mutex> lock(sh_mutex_);
		event_mmap.erase(ev_type);
	}

	// https://demin.ws/blog/english/2012/04/14/return-vector-by-value-or-pointer/
	std::vector<Event> query(std::string ev_type , long int startTime, long int endTime ){
		std::shared_lock<std::shared_mutex> lock(sh_mutex_);

		auto range = event_mmap.equal_range(ev_type);
		if( range.first != range.second ){
			
			std::vector<Event> vect;
			
			std::unordered_multimap<std::string, int>::iterator it = range.first;
			
			while( it != range.second ){
				if( (it->second >= startTime) && (it->second < endTime) ){
					Event ev(ev_type , it->second);
					vect.push_back(ev);
				}
				it++;
			}

			return vect;
		}
		else{
			std::vector<Event> vect;
			return vect;
		}
	}

	void print_mmap(){
		std::shared_lock<std::shared_mutex> lock(sh_mutex_);

		std::unordered_multimap<std::string, int>::iterator it = event_mmap.begin();
 
    for (; it != this->event_mmap.end(); it++)
        std::cout << "<" << it->first << ", " << it->second
                  << ">  \n";
 
    std::cout << std::endl;
	}
};

// -----------------------------------------------------

void thread_fun_0(EventStore *ES,int idx){
	if(idx == 0){
		for(int i=0;i<10;i+=1){
			std::string str_val("event_label_");
			str_val += std::to_string(i%3);
			Event ev(str_val,i);
			ES->insert(ev);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		ES->removeAll("event_label_1");

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		for(int i=373;i<411;i+=1){
			std::string str_val("event_label_");
			str_val+= std::to_string(i%3);
			Event ev(str_val,i);
			ES->insert(ev);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

	} else {
		std::vector<Event> ev_vector;

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		ev_vector = ES->query("event_label_1",0,400);

		std::cout << "queried event vector: \n";
		for(int i=0;i<ev_vector.size();i+=1)
			std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		ev_vector = ES->query("event_label_1",0,400);

		std::cout << "queried event vector: \n";
		for(int i=0;i<ev_vector.size();i+=1)
			std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";

	}
}

// https://livebook.manning.com/book/c-plus-plus-concurrency-in-action-second-edition/chapter-3/1
// https://stackoverflow.com/questions/10661792/how-to-create-an-array-of-thread-objects-in-c11
void parallel_test_0(void){
	const int NUM_THREADS = 2;
	EventStore ES;

	std::thread lthread[NUM_THREADS];

	for(int i=0;i<NUM_THREADS;i++)
		lthread[i] = std::thread(thread_fun_0,&ES,i);

	for(int i=0;i<NUM_THREADS;i++)
		lthread[i].join();

	return ; 
}

#define NUM_EVENTS_TYPES 12

void thread_fun_1(EventStore *ES,int idx){
	if(idx == -1){

		const long int N = 128;
		const int N_batches = 32;

		for(int k=0;k<N_batches;k+=1){
			long int time_shift = 0;

			for(int i=0;i<N;i+=1){
				std::string str_val("event_label_");
				str_val += std::to_string(i%NUM_EVENTS_TYPES);
				Event ev(str_val,time_shift+i);
				ES->insert(ev);
			}	

			// take a small nap
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		}

		for(int k=0;k<NUM_EVENTS_TYPES;k+=1){
			std::string str_val("event_label_");
			str_val += std::to_string(k);

			ES->removeAll("event_label_1");
		}

	} else {
		std::string str_val("event_label_");
		str_val += std::to_string(idx);

		while(true){
			std::vector<Event> ev_vector = ES->query(str_val,0,400);

			if(ev_vector.size() == 0)
				break;

			std::cout << "queried event vector: \n";
			for(int i=0;i<ev_vector.size();i+=1)
				std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";
		}
	}
}

void parallel_test_1(void){
	const int NUM_THREADS = 2;
	EventStore ES;

	std::thread lthread[NUM_THREADS];

	for(int i=0;i<NUM_THREADS;i++)
		lthread[i] = std::thread(thread_fun_0,&ES,i-1);

	for(int i=0;i<NUM_THREADS;i++)
		lthread[i].join();

	return ; 
}

int main(void){
	//test_0();
	//test_1();
	//test_2();
	//test_3();
	//test_4();
	//test_5();
	//test_6();
	parallel_test_0();

	return 0;
}

// ------------------

void test_0(void){
	Event ev("type0",125L);
	std::cout << "type: " << ev.Type() << " - " << ev.Timestamp() << "\n";

	return ; 
}

void test_1(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		Event ev("ABC",i);
		ES.insert(ev);
	}

	ES.print_mmap();

	return ; 
}

void test_2(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		std::string str_val("event_label_");
		//str_val << 'A';
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	return ; 
}

void test_3(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		std::string str_val("event_label_");
		//str_val << 'A';
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	ES.removeAll("event_label_1");

	ES.print_mmap();

	return ; 
}

void test_4(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	ES.removeAll("event_label_1");

	ES.print_mmap();

	for(int i=373;i<411;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	return ; 
}

void test_5(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	ES.removeAll("event_label_1");

	ES.print_mmap();

	for(int i=373;i<411;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	std::vector<Event> ev_vector = ES.query("event_label_0",3,7);

	std::cout << "queried event vector: \n";
	for(int i=0;i<ev_vector.size();i+=1)
		std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";

	return ; 
}

void test_6(void){
	EventStore ES;

	for(int i=0;i<10;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	ES.removeAll("event_label_1");

	ES.print_mmap();

	for(int i=373;i<411;i+=1){
		std::string str_val("event_label_");
		str_val+= std::to_string(i%3);
		Event ev(str_val,i);
		ES.insert(ev);
	}

	ES.print_mmap();

	std::vector<Event> ev_vector = ES.query("event_label_0",3,7);

	std::cout << "queried event vector: \n";
	for(int i=0;i<ev_vector.size();i+=1)
		std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";

	ev_vector = ES.query("event_label_0",370,400);

	std::cout << "queried event vector: \n";
	for(int i=0;i<ev_vector.size();i+=1)
		std::cout << ev_vector[i].Type() << "," << ev_vector[i].Timestamp() << "\n";

	return ; 
}