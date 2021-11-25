#include <iostream>
#include <mutex>
#include <vector>
#include <thread>
#include <string>
#include <shared_mutex>
#include <unordered_map>

/*
 * Event structure that is used to input data 
 * into the EventStore and to receive the 
 * queried data from the EventStore. 
 *
 * Data is not actually stored in this formatted inside EventStore. 
 *
 */

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
 * From EventStoreSharedMutex.cpp file:
 * 
 * The requirement was the following:
 * 
 * "The implementation should be correct, fast, memory-efficient, and thread-safe. 
 *  You may consider that insertions, deletions, queries, and iterations 
 *  will happen frequently and concurrently. This will be a system hotspot. Optimize at will. "
 *
 * This is not feasible to do in an optimized manner if the typical access patterns for
 * reads and writes (queries, insertions/deletions) are not specified. The proper data-structure for a 
 * paralllel read-heavy but serial write-light pattern is very different from a parallel write-heavy and
 * read-light access pattern. 
 * 
 * Also tolerance for latency must be taken into account for write-heavy loads as  allowing (or not) 
 * for buffering writes as an option. Though suggested, it is not stricly defined that the timestamps 
 * arrive monotonically in time, which is another factor that can contribute to the design choices of
 * both the data structure and the parallelization scheme.
 * 
 * I chose here to do a simple implementation of a serial write-lite but parallel read heavy 
 * EventStore "in-memory database". This was done due to the lack of specificity mentioned before and 
 * also out of convenience since the alternatives could be considerably more complex to implement, which 
 * could be troublesome to deliver in the required timeframe. 
 * 
 * I also supose that the ratio of timestamps to possible events is large as to a hashmap indexed by 
 * events instead of the other way arount. I also suppose that the total number of possible events 
 * is limited and preferably not too big. It is expected that several events with the same Event type
 * are to be stored in EventStore. 
 *
 * reference: https://www.geeksforgeeks.org/unordered_multimap-and-its-application/
 * 
 * Under these assumptions, I chose unordered_multimap<string, long int> event_mmap as the base data-structure 
 * for EventStore due to having the desired behavior implemented off the shelf. From this basis, I implemented
 * the appropriate std::shared_mutex to enabled shared-read but serialized write into the event_mmap. 
 *
 * I personally would not choose to write this as a class, I would prefer instead to pass by reference 
 * event_mmap and sh_mutex_ to the function equivalents to the EventStore methods, but in keeping with the 
 * format asked in Java language I structured as such. 
 *
 * For the scenario of multiple-parallel reads/queries but few serial writes, there is a natural mutex available
 * called std::shared_mutex available since C++17 (https://en.cppreference.com/w/cpp/thread/shared_mutex). 
 * 
 * A good explaination of the usefulness of shared_mutex in this scenario can be found in 
 * https://ncona.com/2019/03/read-write-mutex-with-shared_mutex/ with additional references below. 
 * 
 * A mutex array was considered in an implementation focusing in parallel write-heavy loads, but was since 
 * abandoned due to aforementioned reasons. The mutex array idea can be seen in the following reference 
 * (Captain Obvlious) :
 * https://stackoverflow.com/questions/16465633/how-can-i-use-something-like-stdvectorstdmutex
 *
 * I also considered implemented a thread pool in the lines of multiprocessing library from python. 
 * I have since reconsidered since reading the following reference:
 * https://ncona.com/2019/05/using-thread-pools-in-cpp/
 *
 * ----------
 * 
 * Serial version below, which was used as starting point fo the full version. 
 * 
 */

/*
 * https://stackoverflow.com/questions/1601943/mutex-lock-on-write-only
 * https://stackoverflow.com/questions/19915152/c11-multiple-read-and-one-write-thread-mutex
 * https://www.geeksforgeeks.org/c-mutable-keyword/
 *
 */

class EventStore {
private: 
	std::unordered_multimap<std::string, int> event_mmap; // Simplest data structure to this problem, the alternative 
	                                                      // would have been something like
	                                                      // std::unordered_map< std::string, std::vector<int> > event_map;

public:
	void insert(Event in_event){
		event_mmap.insert({ in_event.Type(), in_event.Timestamp() });
	}

	void removeAll(std::string ev_type){
		event_mmap.erase(ev_type);
	}

	// https://demin.ws/blog/english/2012/04/14/return-vector-by-value-or-pointer/
	std::vector<Event> query(std::string ev_type , long int startTime, long int endTime ){
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
		std::unordered_multimap<std::string, int>::iterator it = event_mmap.begin();
 
    for (; it != this->event_mmap.end(); it++)
        std::cout << "<" << it->first << ", " << it->second
                  << ">  \n";
 
    std::cout << std::endl;
	}
};

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

int main(void){
	//test_0();
	//test_1();
	//test_2();
	//test_3();
	//test_4();
	//test_5();
	test_6();

	return 0;
}