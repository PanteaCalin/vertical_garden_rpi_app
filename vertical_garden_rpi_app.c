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

	time_t sleep_sec = 0;

	struct tm *time_broken_down;

	// run thread in infinite loop
	while(1) {
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

		unsigned int intervals;
		unsigned int i;

		printf("current_sec: %d\n", current_sec);
		printf("start_sec:   %d\n", start_sec);
		printf("end_sec:     %d\n", end_sec);

		// Legend:
		// xxx : active period
		// --- : inactive period

		// 00:00      start   end        23:59
		//   |----------|xxxxxx|-----------|
		if(start_sec < end_sec) {
			//printf("dbg#0\n");
			// 00:00      start   end        23:59
			//   |----------|xxxxxx|-----------|
			//		  ^
			//     current
			if(current_sec < start_sec) {
				// put thread to sleep until the start of active period
				//printf("dbg#1\n");
				sleep_sec = start_sec - current_sec;
				goto put_thread_to_sleep;
			}

			// 00:00      start   end        23:59
			//   |----------|xxxxxx|-----------|
			//		            ^
			//               current
			if((current_sec >= start_sec) &&
			   (current_sec <= end_sec)) {
				// current time is inside the active period
				// calculate the intervals at which the thread should wake-up while in active period
				//printf("dbg#2\n");
				intervals = (end_sec - start_sec) / (task.freq * 60);

				// determine where the current time is with respect to task wake-up intervals
				for(i=0; i<intervals; i++) {
					//printf("dbg#3\n");
					if( current_sec >= (start_sec + (i * (task.freq * 60)) ) &&
						current_sec < (start_sec + (i + 1) * (task.freq * 60)) ) {
						//printf("dbg#4\n");
						if (current_sec == (start_sec + (i * (task.freq * 60))) ) {
							//printf("dbg#5\n");
							// 00:00  start  i  i+1 i+2..i+n end     23:59
							//   |-----|xxxxx|xxx|xxx|xxx|xxx|--------|
							//               ^
							//            current
							// execute task
							// assert(task.duration < (task.freq * 60))
							// gpio ON
							// sleep 'duration' sec
							// gpio OFF
							bcm2835_gpio_fsel(4, BCM2835_GPIO_FSEL_OUTP);	
							bcm2835_gpio_fsel(18, BCM2835_GPIO_FSEL_OUTP);	

							printf("gpio 4 set\n");
							bcm2835_gpio_set(4);

							printf("gpio 18 set\n");
							bcm2835_gpio_set(18);

							sleep(task.duration);

							printf("gpio 4 clr\n");
							bcm2835_gpio_clr(4);

							printf("gpio 18 clr\n");
							bcm2835_gpio_clr(18);

							sleep_sec = task.freq * 60 - task.duration;
							goto put_thread_to_sleep;

						} else {
							// 00:00  start  i  i+1 i+2..i+n end     23:59
							//   |-----|xxxxx|xxx|xxx|xxx|xxx|--------|
							//                 ^
							//              current
							//printf("dbg#6\n");
							// sleep until next interval
							sleep_sec = (start_sec + (i + 1) * (task.freq * 60)) - current_sec;
							goto put_thread_to_sleep;
						}
					} else {
						// set the next wake-up to next start but don't jump to 'put_threa_to_sleep'
						// this setting is just a safety net in case no valid interval is found in the 'for' loop
						sleep_sec = (start_sec + 86400) - current_sec;
					}
				}
			}

			// 00:00      start   end        23:59
			//   |----------|xxxxxx|-----------|
			//		                     ^
			//                        current
			if(current_sec > end_sec) {
				//printf("dbg#7\n");
				// put thread to sleep until the next day, start time; 24*60*60 = 86400 sec;
				sleep_sec = (start_sec + 86400) - current_sec;
				goto put_thread_to_sleep;
			}

		// 00:00       end   start        23:59
		//   |xxxxxxxxxx|------|xxxxxxxxxxxx|
		} else if(start_sec > end_sec) {
			//printf("dbg#8\n");

			// 00:00       end   start        23:59
			//   |xxxxxxxxxx|------|xxxxxxxxxxxx|
			//                           ^
			//                        current
			if(current_sec >= start_sec) {
				//printf("dbg#9\n");
				intervals = ((end_sec + 86400) - start_sec) / (task.freq * 60);
				for(i=0; i<intervals; i++) {
					//printf("dbg#10\n");
					if( (current_sec >= (i    * (task.freq * 60) + start_sec)) &&
						(current_sec < ((i+1) * (task.freq * 60) + start_sec))) {
						if( current_sec == (i * (task.freq * 60) + start_sec) ) {
							// execute task
							//printf("dbg#11\n");
							bcm2835_gpio_fsel(4, BCM2835_GPIO_FSEL_OUTP);	
							bcm2835_gpio_fsel(18, BCM2835_GPIO_FSEL_OUTP);	

							printf("gpio 4 set\n");
							bcm2835_gpio_set(4);

							printf("gpio 18 set\n");
							bcm2835_gpio_set(18);

							sleep(task.duration);

							printf("gpio 4 clr\n");
							bcm2835_gpio_clr(4);

							printf("gpio 18 clr\n");
							bcm2835_gpio_clr(18);

							sleep_sec = task.freq * 60 - task.duration;
							goto put_thread_to_sleep;
						} else {
							//printf("dbg#12\n");
							sleep_sec = ((i+1) * (task.freq * 60) + start_sec) - current_sec;
							goto put_thread_to_sleep;
						}
					}
				}
			}

			if(current_sec <= end_sec) {
				//printf("dbg#13\n");
				intervals = (end_sec - (start_sec - 86400)) / (task.freq * 60);
				for(i=0; i<intervals; i++) {
					//printf("dbg#14\n");
					if( (current_sec >= (i    * (task.freq * 60) + (start_sec - 86400))) &&
						(current_sec < ((i+1) * (task.freq * 60) + (start_sec - 86400)))) {
						if( current_sec == (i * (task.freq * 60) + (start_sec - 86400)) ) {
							// execute task
							//printf("dbg#15\n");
							bcm2835_gpio_fsel(4, BCM2835_GPIO_FSEL_OUTP);	
							bcm2835_gpio_fsel(18, BCM2835_GPIO_FSEL_OUTP);	

							printf("gpio 4 set\n");
							bcm2835_gpio_set(4);

							printf("gpio 18 set\n");
							bcm2835_gpio_set(18);

							sleep(task.duration);

							printf("gpio 4 clr\n");
							bcm2835_gpio_clr(4);

							printf("gpio 18 clr\n");
							bcm2835_gpio_clr(18);

							sleep_sec = task.freq * 60 - task.duration;
							goto put_thread_to_sleep;
						} else {
							//printf("dbg#16\n");
							sleep_sec = ((i+1) * (task.freq * 60) + (start_sec - 86400)) - current_sec;
							goto put_thread_to_sleep;
						}
					}
				}
			}

			if((current_sec > end_sec) && (current_sec < start_sec)) {
				//printf("dbg#17\n");
				// put thread to sleep until start time
				sleep_sec = start_sec - current_sec;
				goto put_thread_to_sleep;
			}

		// 00:00    start/end             23:59
		//   |xxxxxxxxxxx|xxxxxxxxxxxxxxxxxx|
		} else if (start_sec == end_sec) {
			//printf("dbg#x\n");
		}

	put_thread_to_sleep:
			print_safe(task.id, &logfile_mutex, "task #,%d, going to sleep for ,%d, sec (,%d, min)\n", 3, task.id, sleep_sec, sleep_sec/60);
			// put thread to sleep until the next scheduled wake-up
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

