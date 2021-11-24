#include <iostream>
#include <mutex>
#include <vector>
#include <thread>
#include <string>
#include <shared_mutex>
#include <unordered_map>

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
 * mutex pool reference
 * https://stackoverflow.com/questions/16465633/how-can-i-use-something-like-stdvectorstdmutex
 */

// https://www.geeksforgeeks.org/unordered_multimap-and-its-application/
// https://en.cppreference.com/w/cpp/container/unordered_multimap/insert

// unordered_multimap<string, int>::iterator

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