#include <mysql.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <stdarg.h>
#include "bcm2835.h"
#include "vertical_garden_rpi_app.h"

/******************************************
 * 				Defines
 *******************************************/
#define PERIODIC_TASKS_NO   6
#define TASK_ID_POS			0
#define TASK_ACTIVE_POS  	1
#define TASK_START_TIME_POS 2
#define TASK_END_TIME_POS	3
#define TASK_FREQ_POS		4
#define TASK_DURATION_POS	5

#define LOG_FILE "log_file.csv"

/******************************************
 * 				Global Variables
 *******************************************/
struct periodic_task {
	unsigned int id;
	unsigned int start_hour;
	unsigned int start_min;
	unsigned int end_hour;
	unsigned int end_min;
	unsigned int freq;
	unsigned int duration;
};

struct periodic_task *periodic_tasks[PERIODIC_TASKS_NO];

pthread_mutex_t logfile_mutex;

const unsigned int task_gpios[6] = {4, 0, 0, 0, 0, 0};


const struct time_segment {
	time_t        segment_start;
	time_t        segment_end;
	unsigned char active;
};

/******************************************
 * 		   Function Prototypes
 *******************************************/
static void print_safe(unsigned int task_id, pthread_mutex_t* mutex, char* msg, int argn, ...);

/******************************************
 * update_periodic_tasks_from_database()
 *******************************************/
static void update_periodic_tasks_from_database(void)
{
	MYSQL *conn;
	MYSQL_RES *res;
	MYSQL_ROW *row;
	int i;

	//printf("MySQL Database connection initiated\n");
	print_safe(0, &logfile_mutex, "MySQL Database connection initiated\n", 0);

	conn = mysql_init(NULL);

	if(!mysql_real_connect(conn, db_server, db_user, db_password, db_database, 0, NULL, 0)) {
		fprintf(stderr, "%s\n", mysql_error(conn));
		print_safe(0, &logfile_mutex, "MySQL DB Connect Error: %s\n", 1, mysql_error(conn));
		exit(1);
	}

	// SELECT * FROM irrigation_table
	if(mysql_query(conn, "SELECT * FROM irrigation_table")) {
		fprintf(stderr, "%s\n", mysql_error(conn));
		print_safe(0, &logfile_mutex, "MySQL DB Query Error: %s\n", 1, mysql_error(conn));
		exit(3);
	}

	res = mysql_use_result(conn);

	// reset periodic tasks
	for(i=0; i<PERIODIC_TASKS_NO; i++)
		periodic_tasks[i] = NULL;

	// update periodic tasks with database parameters
	i=0;
	// mysql_fetch_row() returns the next database row in the form of an array of strings
	while((row = mysql_fetch_row(res)) != NULL) {
		// watch out not to exceed the limit
		if(i >= PERIODIC_TASKS_NO) {
			break;
		}
		// process only the enabled tasks
		if(atoi(row[TASK_ACTIVE_POS])) {
			periodic_tasks[i] = (struct periodic_task*)malloc(sizeof(struct periodic_task));
			if(periodic_tasks[i] != NULL) {
				periodic_tasks[i]->id         = (unsigned int)atoi(row[TASK_ID_POS]);
				periodic_tasks[i]->freq       = (unsigned int)atoi(row[TASK_FREQ_POS]);
				periodic_tasks[i]->duration   = (unsigned int)atoi(row[TASK_DURATION_POS]);
				// start time read from database in the "hh:mm:ss" format
				periodic_tasks[i]->start_hour = (unsigned int)atoi(strtok(row[TASK_START_TIME_POS], ":"));
				periodic_tasks[i]->start_min  = (unsigned int)atoi(strtok(NULL, ":"));
				// end time read from database in the "hh:mm:ss" format
				periodic_tasks[i]->end_hour   = (unsigned int)atoi(strtok(row[TASK_END_TIME_POS], ":"));
				periodic_tasks[i]->end_min    = (unsigned int)atoi(strtok(NULL, ":"));
				i++;
			} else {
				fprintf(stderr, "ERROR: periodic task malloc failed; row id #%d, task id #%d\n", i, row[TASK_ID_POS]);
				print_safe(0, &logfile_mutex, "MySQL ERROR: periodic task malloc failed; row id #%d, task id #%d\n", 2, i, row[TASK_ID_POS]);
			}
		}
	}

	mysql_free_result(res);
	mysql_close(conn);

	printf("MySQL Database connection done\n");
	print_safe(0, &logfile_mutex, "MySQL Database connection done\n", 0);
	// return 0; // void
}

/******************************************
 * print_safe()
 * params: - unsigned int task_id: id of the taks calling this function;
 * 								 primarily used for identifiying the thread in case of failure
 * 		   - pthread_mutex_t* mutex: pointer to the mutex used to protect the logfile
 * 		   - char* msg: pointer to the string to be written to logfile
 * 		   - int argn: number of arguments given in the elipses ('...') argument
 * 		   - ... : variable number of arguments (the number of arguments passed should match 'argn' parameter)
 *******************************************/
static void print_safe(unsigned int task_id, pthread_mutex_t* mutex, char* msg, int argn, ...)
{
	time_t timestamp_sec;
	struct tm *timestamp;
	FILE* fp;

	// begin of mutex-protected area
	pthread_mutex_lock(mutex);

	// open log file
	fp = fopen(LOG_FILE, "a+");
	if(fp == NULL) {
		printf("ERROR: task #%d: could not open log file\n", task_id);
		exit(1);
	}

	timestamp_sec = time(NULL);
	timestamp = localtime(&timestamp_sec);
	va_list args;
	va_start(args, argn);
	// print timestamp
	fprintf(fp, "%u,-,%d:%d:%d:%d:%d:%d:, ", timestamp_sec,
											 timestamp->tm_year + 1900,
											 timestamp->tm_mon + 1,
											 timestamp->tm_mday,
											 timestamp->tm_hour,
											 timestamp->tm_min,
											 timestamp->tm_sec);
	vfprintf(fp, msg, args);
	va_end(args);

	// close log file
	fclose(fp);
	// end of mutex-protected area
	pthread_mutex_unlock(mutex);
}

/******************************************
 * execute_task()
 *******************************************/
static void execute_task(const struct periodic_task* task)
{

}

/******************************************
 * run_periodic_task()
 *******************************************/
static void *run_periodic_task(void *arg)
{
	struct periodic_task task = *(struct periodic_task *)arg;

	time_t current_time_sec;
	time_t midnight_time_sec;
	time_t task_start_time_sec;
	time_t task_end_time_sec;
	time_t task_next_wake_up_sec;

	time_t start_sec;
	time_t end_sec;
	time_t current_sec;

	time_t interval_start;
	time_t interval_end;

	time_t sleep_sec = 0;

	struct tm *time_broken_down;

	struct time_segment time_segments[3];

	unsigned int intervals;
	unsigned int i, s;

	// run thread in infinite loop
	while(1) {

		// get current time in seconds since epoch (01.01.1970, 00:00:00)
		current_time_sec = time(NULL);
		printf("current_time_sec: %d\n", current_time_sec);
		// get current time broken down
		time_broken_down = localtime(&current_time_sec);
		printf("localtime: %d:%d:%d:%d:%d:%d\n", time_broken_down->tm_year + 1900,
												 time_broken_down->tm_mon + 1,
												 time_broken_down->tm_mday,
												 time_broken_down->tm_hour,
												 time_broken_down->tm_min,
												 time_broken_down->tm_sec);
		// get midnight time in sec
		time_broken_down->tm_hour = 0;
		time_broken_down->tm_min = 0;
		midnight_time_sec = mktime(time_broken_down);

		// get task start time in sec
		time_broken_down->tm_hour = task.start_hour;
		time_broken_down->tm_min = task.start_min;
		time_broken_down->tm_sec = 0; //NOTE: start and end time do not supprt seconds resolution
		task_start_time_sec = mktime(time_broken_down);

		// set task end time in sec
		time_broken_down->tm_hour = task.end_hour;
		time_broken_down->tm_min = task.end_min;
		time_broken_down->tm_sec = 0; //NOTE:
		task_end_time_sec = mktime(time_broken_down);

		current_sec = current_time_sec;
		start_sec   = task_start_time_sec;
		end_sec     = task_end_time_sec;

		printf("current_sec: %d\n", current_sec);
		printf("start_sec:   %d\n", start_sec);
		printf("end_sec:     %d\n", end_sec);

		// Algorithm rationale:
		//                         00:00:00                   23:59:59
		// ||<------- DAY n-1 ------->||<--------- DAY n ------->||<------- DAY n+1 ------->|| }
		// ||                         ||                         ||                         || }
		// ||    start      end       ||    start      end       ||     start     end       || } case 1: start < end
                // ||______|_________|________||______|_________|________||______|_________|________|| }
		// ||      ^^^^^^^^^^^        ||      ^^^^^^^^^^^        ||      ^^^^^^^^^^^        || }
		// ||      | active  |        ||      | active  |        ||      | active  |        || }
                // ||      |         |        ||      |         |        ||      |         |        ||
		// ||      |         |        ||      |         |        ||      |         |        || }
		// ||     end      start      ||     end      start      ||     end      start      || }
	        // ||______|_________|________||______|_________|________||______|_________|________|| } case 2: start > end
		// ^^^^^^^^^         ^^^^^^^^^^^^^^^^^^         ^^^^^^^^^^^^^^^^^^         ^^^^^^^^^^^ }
                //   active          |      active    |         |      active    |             active  }
                //                   |                |         |                |                     }
		//                   ^^^^^^^^^^^^^^^^^^         |                |
		//                       SEGMENT_0    ^^^^^^^^^^^                |
		//                                    SEGMENT_1  ^^^^^^^^^^^^^^^^^
                //                                                 SEGMENT_2

		// calculate time segments as per the above diagram
		// case 1
		if(start_sec < end_sec){
			// time segment_0
			time_segments[0].segment_start = end_sec - 86400;
			time_segments[0].segment_end   = start_sec;
			time_segments[0].active        = 0;
			// time segment_1
			time_segments[1].segment_start = start_sec;
			time_segments[1].segment_end   = end_sec;
			time_segments[1].active        = 1;
			// time segment_2
			time_segments[2].segment_start = end_sec;
			time_segments[2].segment_end   = start_sec + 86400;
			time_segments[2].active        = 0;
		// case 2
		} else if(start_sec > end_sec){
			// time segment_0
			time_segments[0].segment_start = start_sec - 86400;
			time_segments[0].segment_end   = end_sec;
			time_segments[0].active        = 1;
			// time segment_1
			time_segments[1].segment_start = end_sec;
			time_segments[1].segment_end   = start_sec;
			time_segments[1].active        = 0;
			// time segment_2
			time_segments[2].segment_start = start_sec;
			time_segments[2].segment_end   = end_sec + 86400;
			time_segments[2].active        = 1;
		// case 3
		} else {
			// active period is around the clock
		}

		// set the default sleep time for the current thread
		sleep_sec = 60;

		// iterate through all the time segments and determine in which one the current time fits in
		for(s=0; s<3; s++){
			// active segment
			if( (current_sec >= time_segments[s].segment_start) &&
				(current_sec <= time_segments[s].segment_end)   &&
				(time_segments[s].active == 1)) {
				// calculate the wake-up intervals across the active segment
				intervals = (unsigned int)(time_segments[s].segment_end -
						                   time_segments[s].segment_start) /
						                   (task.freq * 60);
				// determine in which of these intervals the current time is
				for(i=0; i<intervals; i++){
					interval_start = ( i    * task.freq * 60) + time_segments[s].segment_start;
					interval_end   = ((i+1) * task.freq * 60) + time_segments[s].segment_start;
					// found a valid interval inside the active time segment
					if(current_sec >= interval_start &&
					   current_sec <  interval_end) {
						// determine now if the current time perfectly matches the start of the interval
						// or if the thread need to be put to sleep until the start of the next interval
						if(current_sec == interval_start) {
							// execute task
							execute_task(&task);
							sleep_sec = interval_end - task.duration;
						} else {
							// put thread to sleep until next interval start
							sleep_sec = interval_end - current_sec;
						}
						// break the for loop;
						break;
					}
				}
			// inactive segment
			} else if ( (current_sec > time_segments[s].segment_start) &&
					    (current_sec < time_segments[s].segment_end)   &&
					    (time_segments[s].active == 0) ) {

				switch(s){
				case 0: sleep_sec = time_segments[1].segment_start - current_sec; break;
				case 1: sleep_sec = time_segments[2].segment_start - current_sec; break;
				case 2: sleep_sec = (time_segments[1].segment_start + 86400) - current_sec; break;
				default: // we're in trouble if program flow choses this case
					break;
				}
			}
		}
		// put thread to sleep until the next scheduled wake-up
		print_safe(task.id, &logfile_mutex, "task #,%d, going to sleep for ,%d, sec (,%d, min)\n", 3, task.id, sleep_sec, sleep_sec/60);
		sleep(sleep_sec);
		print_safe(task.id, &logfile_mutex, "task #,%d, woke up\n", 1, task.id);
	 }
}

/******************************************
 * main()
 *******************************************/
int main() {

	pthread_t thead_id_access_db;
	pthread_t thread_id_periodic_tasks[PERIODIC_TASKS_NO];
	unsigned int i;

	pthread_mutex_init(&logfile_mutex, NULL);

	print_safe(0, &logfile_mutex, "Application started\n", 0);

	printf("bcm2835_init result: %d\n", bcm2835_init());

	// update periodic tasks parameters from database
	update_periodic_tasks_from_database();

	// run each periodic task in it's own pthread
	for(i=0; i<PERIODIC_TASKS_NO; i++) {
		if(periodic_tasks[i] != NULL) {
			if(pthread_create(&thread_id_periodic_tasks[i],
							  NULL,
							  &run_periodic_task,
							  (void*)periodic_tasks[i])) {
				fprintf(stderr, "Error creating thread periodic task #%d\n", i);
				exit(3);
			}
		}
	}

	// wait for all started pthreads
	for(i=0; i<PERIODIC_TASKS_NO; i++) {
		if(periodic_tasks[i] != NULL) {
			if(pthread_join(thread_id_periodic_tasks[i], NULL)) {
				fprintf(stderr, "Error joining thread #%d\n", i);
				exit(2);
			}
		}
	}
 return 0;
}

