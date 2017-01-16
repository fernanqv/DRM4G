/* -------------------------------------------------------------------------- */
/* Copyright 2002-2011, GridWay Project Leads (GridWay.org)                   */
/*                                                                            */
/* Licensed under the Apache License, Version 2.0 (the "License"); you may    */
/* not use this file except in compliance with the License. You may obtain    */
/* a copy of the License at                                                   */
/*                                                                            */
/* http://www.apache.org/licenses/LICENSE-2.0                                 */
/*                                                                            */
/* Unless required by applicable law or agreed to in writing, software        */
/* distributed under the License is distributed on an "AS IS" BASIS,          */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   */
/* See the License for the specific language governing permissions and        */
/* limitations under the License.                                             */
/* -------------------------------------------------------------------------- */

#include "gw_client.h"
#include "gw_scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <syslog.h>
#include <pthread.h>
#include <sys/queue.h>

static void gw_scheduler_init (gw_scheduler_t * sched);

extern gw_client_t gw_client;
int  gw_client_connect();
void gw_client_disconnect(int socket);

static FILE *fd_log;

#define GW_SCHED_FIELD_WIDTH 80
#define GW_SCHED_MSG_WIDTH   480

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

typedef struct _qsentence {                                          
	char    act[GW_SCHED_FIELD_WIDTH];
        char    id1[GW_SCHED_FIELD_WIDTH];
        char    id2[GW_SCHED_FIELD_WIDTH];
        char    at1[GW_SCHED_FIELD_WIDTH];
        char    at2[GW_SCHED_FIELD_WIDTH];
        char    at3[GW_SCHED_FIELD_WIDTH];                              
        STAILQ_ENTRY(_qsentence) pointers;                           
} qsentence;                                                         
 

STAILQ_HEAD(queue_sched, _qsentence) qsched = STAILQ_HEAD_INITIALIZER( qsched );
pthread_mutex_t mutex;

gw_scheduler_t sched;

gw_client_t *         gw_session = NULL;
gw_return_code_t      gwrc;
gw_migration_reason_t reason;
gw_msg_job_t          job_status;

void gw_scheduler_loop(gw_scheduler_function_t scheduler, void *user_arg)
{      

        int     fd, rc, i;
 
        int     error = 0;       
 
        time_t  deadline, the_time;
               
        int     hid, uid, jid, aid, np;
        int     uslots, ajobs, fixed_priority;
        int     rjobs;
        float   txfr,texe,tsus;
 
        char *  name;
        pthread_t thread;

        char *  GW_LOCATION;
        char *  log;
        char *  conf;
        char *  error_str;

        int     length;


        STAILQ_INIT(&qsched);
        pthread_mutex_init(&mutex, NULL);
        pthread_create( &thread, NULL, &gw_scheduler_dispacher, NULL);
 
        qsentence *qstr; 

        /* ------------------------------- */
        /*  Init environment ang log file  */
        /* ------------------------------- */

        GW_LOCATION = getenv("GW_LOCATION");

        if(GW_LOCATION == NULL)
        {
                error = 1;
                error_str = strdup("GW_LOCATION environment variable is undefined.");

                gw_scheduler_print('E',"%s\n",error_str);
        }
        else
        {
                length = strlen(GW_LOCATION);

                log =(char *) malloc(sizeof(char)*(length + sizeof(GW_VAR_DIR) + 12));
                conf=(char *) malloc(sizeof(char)*(length + sizeof(GW_ETC_DIR) + 13));

                sprintf(log, "%s/" GW_VAR_DIR "/sched.log", GW_LOCATION);
                sprintf(conf,"%s/" GW_ETC_DIR "/sched.conf",GW_LOCATION);

#ifndef GWSYSLOG
                rc = truncate(log, 0);
                fd_log = fopen(log,"a");

                if (fd_log == NULL)
                {
                        error     = 1;
                        error_str = strdup(strerror(errno));

                        gw_scheduler_print('E',"Could not open file %s - %s\n",log,error_str);
                }
                else
                        setbuf(fd_log,NULL);

                free(log);
#else
                openlog("GridWayShed", LOG_PID, GWSYSLOG);
#endif
        }

        setbuf(stdout,NULL);

        /* ----------------------------------- */
        /*  Scheduler Initialization           */
        /* ----------------------------------- */

        gw_scheduler_init(&sched);

        gw_sch_conf_init(&(sched.sch_conf));

        rc = gw_sch_loadconf(&(sched.sch_conf), conf);

        if (rc != 0 )
        {
                gw_scheduler_print('E',"Parsing scheduler configuration file %s, using defaults.\n",conf);
        }

        free(conf);

        the_time = time(NULL);

        sched.next_user_window = the_time +
                            (time_t) (sched.sch_conf.window_size * 86400);

        sched.next_host_window = the_time + (time_t) 86400;

        gw_scheduler_print('I',"Scheduler successfully started.\n");




	for(;;)
        {
                pthread_mutex_lock(&mutex);

                if( STAILQ_EMPTY(&qsched) )
                {
                        pthread_mutex_unlock(&mutex);
			sleep(5);
			continue;
		}
		qstr = STAILQ_FIRST(&qsched);        
                STAILQ_REMOVE_HEAD(&qsched, pointers);           
		pthread_mutex_unlock(&mutex);

		if (strcmp(qstr->act, "INIT") == 0 )
        	{        	
        		if (error == 0)
                	{
        	    		printf("INIT - SUCCESS -\n");
                	}
        		else
        		{
        			printf("INIT - FAILURE -\n");
        		}
        	}
        	else if (strcmp(qstr->act, "FINALIZE") == 0 )
        	{
                	gw_client_disconnect(fd);
       			gw_client_finalize();
       		
        		printf("FINALIZE - SUCCESS -");
        	}
        	else if (strcmp(qstr->act, "HOST_MONITOR") == 0 )
        	{
        		/* Add or update a given host:
         		* HOST_MONITOR HID USLOTS RJOBS NAME - 
         		*/
			hid    = atoi(qstr->id1);
			uslots = atoi(qstr->id2);
	        	rjobs  = atoi(qstr->at1);
			name   = qstr->at2;
            
			gw_scheduler_add_host(&sched,hid,uslots,rjobs,name);
        	}
        	else if (strcmp(qstr->act, "USER_ADD") == 0 )
        	{
        		/* Add a user:
         		* USER_ADD UID ASLOTS RSLOTS NAME - 
         		*/        	
        		uid   = atoi(qstr->id1);
        		ajobs = atoi(qstr->id2);
        		rjobs = atoi(qstr->at1);
        		name  = qstr->at2;
        	
        		gw_scheduler_add_user(&sched,uid,ajobs,rjobs,name);
        	}
        	else if (strcmp(qstr->act, "USER_DEL") == 0 )
        	{
        		/* Remove an user
         		* USER_DEL UID - - - - 
         		*/        	
        		uid   = atoi(qstr->id1);
        	        	
        		gw_scheduler_del_user(&sched,uid);
        	}        
        	else if (strcmp(qstr->act, "JOB_DEL") == 0 )
        	{
        		/* Remove an job
        	 	* JOB_DEL JID - - - - 
        		*/        	
        		jid   = atoi(qstr->id1);
        	
        		gw_scheduler_job_del(&sched,jid,0);
        	}          
        	else if (strcmp(qstr->act, "JOB_FAILED") == 0 )
        	{
        		/* A job has failed, update user host statistics
         		* JOB_FAILED HID UID REASON - -
         		*/
          		hid    = atoi(qstr->id1);
          		uid    = atoi(qstr->id2);
          		reason = (gw_migration_reason_t) atoi(qstr->at1);
          	
          		gw_scheduler_job_failed(&sched,hid,uid,reason);
        	}                
        	else if (strcmp(qstr->act, "JOB_SUCCESS") == 0 )        
        	{
        		/* A job has been successfully executed, update user host statistics
		         * JOB_SUCCESS HID UID XFR SUS EXE 
         		*/
          		hid  = atoi(qstr->id1);
	          	uid  = atoi(qstr->id2);
          	
	          	txfr = atof(qstr->at1);
	          	tsus = atof(qstr->at2);
        	  	texe = atof(qstr->at3);
            
			gw_scheduler_job_success(&sched,hid,uid,txfr,tsus,texe);
        	}
        	else if (strcmp(qstr->act, "JOB_SCHEDULE") == 0 )        
        	{        	
        		/* A job need schedule add it to the job list
        		 * JOB_SCHEDULE JID AID UID REASON -
         		*/
	          	jid    = atoi(qstr->id1);
        	  	aid    = atoi(qstr->id2);
         		uid    = atoi(qstr->at1);
          		reason = (gw_migration_reason_t) atoi(qstr->at2);

			if (gw_session == NULL)
			{
				gw_session = gw_client_init();

                        	if (gw_session == NULL)
                        	{
                        		gw_scheduler_print('E',"Error creating a GW session.\n");
                        	}


                        	fd = gw_client_connect();

                        	if (fd == -1)  gw_scheduler_print('E',"Error connecting to gwd.\n");
                	}         


            		gwrc = gw_client_job_status_fd(fd, jid, &job_status);

            		if ( gwrc == GW_RC_SUCCESS )
            		{
                		fixed_priority = job_status.fixed_priority;
                		np             = job_status.np;
                		deadline       = job_status.start_time + job_status.deadline;
            		}
            		else
            		{
                		gw_scheduler_print('E',"Error getting job information, will use default values.\n");
                		fixed_priority = 0;
          			np             = 1;
                		deadline       = 0;
            		}
          	
            		gw_scheduler_job_add(&sched,jid,aid,np,reason,
                		    fixed_priority,uid,deadline);
        	}
        	else if (strcmp(qstr->act, "SCHEDULE") == 0 )
        	{
#ifdef GWSCHEDDEBUG
			gw_scheduler_print('D',"JOBS:%i HOSTS:%i USERS:%i\n",
			sched.num_jobs,sched.num_hosts,sched.num_users);
#endif        	
        		if (    (sched.num_hosts > 0) 
        	     		&& (sched.num_users > 0) 
        	     		&& (sched.num_jobs  > 0))
        		{
        	    		for (i=0;i<sched.num_hosts;i++)
        	        		sched.hosts[i].dispatched = 0;

        	    		for (i=0;i<sched.num_users;i++)
        	        		sched.users[i].dispatched = 0;        	
        	
        	    		gw_scheduler_matching_arrays(fd, &sched);
        	    
        	    		if (sched.sch_conf.disable == 0)
        	       			gw_scheduler_job_policies (&sched);
        	
       		    		scheduler(&sched,&user_arg);
        		}
        	
			the_time = time(NULL);

            
        		if ( the_time > sched.next_user_window)
        		{
#ifdef GWSCHEDDEBUG
                		gw_scheduler_print('D',"Updating window shares.\n");
#endif
       		 		sched.next_user_window = time(NULL) + 
       			                     (time_t) (sched.sch_conf.window_size * 86400);
       		                        
        			gw_scheduler_user_update_windows(&sched);
        		}
#ifdef HAVE_LIBDB        	
        		if ( the_time > sched.next_host_window)
        		{
#ifdef GWSCHEDDEBUG
                		gw_scheduler_print('D',"Updating host usage.\n");
#endif	
        			sched.next_host_window = time(NULL) + (time_t) 86400;
       		                     
				gw_scheduler_update_usage_host(&sched);
        		}
#endif       		
       			printf("SCHEDULE_END - SUCCESS -\n");
        	}
        	else
        	{
            		gw_scheduler_print('E',"Unknown action from core %s\n.",qstr->act);        	
        	}
	}
    
    	if (error == 0)
        fclose(fd_log);
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

static void gw_scheduler_init (gw_scheduler_t * sched)
{  
    sched->num_users = 0;
    sched->users     = NULL;
  
    sched->num_hosts = 0;
    sched->hosts     = NULL;

    sched->num_jobs  = 0;
    sched->jobs      = NULL;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

void gw_scheduler_print (const char mode, const char *str_format,...)
{
#ifndef GWSYSLOG

    time_t  the_time;
    
    char str[26];
#else
    char *str_syslog;

    str_syslog = (char*) malloc(sizeof(char)*(10+strlen(str_format)));
    sprintf(str_syslog, "[%c] %s", mode, str_format);
#endif

    va_list ap;
    va_start(ap, str_format);

#ifndef GWSYSLOG
    if (fd_log != NULL)
    {
        the_time = time(NULL);

#ifdef GWSOLARIS
        ctime_r(&(the_time),str,sizeof(char)*26);
#else
        ctime_r(&(the_time),str);
#endif

        str[24]='\0';

        fprintf(fd_log,"%s [%c]: ", str, mode);
        vfprintf(fd_log,str_format,ap);
    }
#else
    switch(mode)
    {

    case 'I':
    	vsyslog(LOG_INFO, str_syslog, ap);
    	break;

    case 'E':
    	vsyslog(LOG_ERR, str_syslog, ap);
    	break;
    case 'W':
    	vsyslog(LOG_WARNING, str_syslog, ap);
    	break;
    case 'D':
    	vsyslog(LOG_DEBUG, str_syslog, ap);
    	break;
    }

    free(str_syslog);

#endif
    return;
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
                                
void gw_scheduler_ctime(time_t the_time, char *str)
{
	int i;
	
#ifdef GWSOLARIS
        ctime_r(&(the_time),str,sizeof(char)*26);
#else
        ctime_r(&(the_time),str);
#endif

	for (i=0;i<8;i++)
		str[i]=str[i+11];
		
	str[8]='\0';
}

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

void * gw_scheduler_dispacher( )
{
        int     end   = 0;
        int     error = 0;

        fd_set  in_pipes;
        int     rc,j;
        char    c;
       
	time_t  the_time;

        char    str[GW_SCHED_MSG_WIDTH];

        /* ----------------------------------- */
        /*  Scheduler Loop                     */
        /* ----------------------------------- */

        while (!end)
        {
                FD_ZERO(&in_pipes);
                FD_SET (0,&in_pipes);

                rc = select(1, &in_pipes, NULL, NULL, NULL);

                if (rc == -1)
                {
                        end = 1;
                        gw_scheduler_print('E',"Error in select() - %s\n",strerror(errno));
                }

                j = 0;

                do
                {
                        rc = read(0, (void *) &c, sizeof(char));
                        str[j++] = c;

                }while ( rc > 0 && c != '\n' );

                str[j] = '\0';

                if (rc <= 0)
                {
                        end = 1;
                }

                qsentence *qdata = (qsentence *)malloc(sizeof(qsentence));
                rc = sscanf(str,"%s %s %s %s %s %s %[^\n]",qdata->act,qdata->id1,qdata->id2,qdata->at1,qdata->at2,qdata->at3);
                pthread_mutex_lock(&mutex);
                STAILQ_INSERT_TAIL(&qsched, qdata, pointers);
                pthread_mutex_unlock(&mutex);
#ifdef GWSCHEDDEBUG
        	gw_scheduler_print('D',"Message received from gwd \"%s %s %s %s %s %s\"\n", qdata->act,qdata->id1,qdata->id2,qdata->at1,qdata->at2,qdata->at3);
#endif

        }
}
