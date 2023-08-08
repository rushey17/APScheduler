from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from pydantic import BaseModel
import datetime, time
from typing import List
from typing_extensions import Literal
import uvicorn as uvicorn
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

jobstores = {
    'default': SQLAlchemyJobStore(url='postgresql://postgres:postgres@localhost:5432/apscheduler')
}

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}


app = FastAPI()
Schedule = None

@app.on_event("startup")
async def load_schedule_or_create_blank():
    """
    Instatialise the Schedule Object as a Global Param and also load existing Schedules from SQLite
    This allows for persistent schedules across server restarts. 
    """
    global Schedule
    try:
        jobstores = {
            'default': SQLAlchemyJobStore(url='postgresql://postgres:postgres@localhost:5432/apscheduler', tablename='apscheduler_jobs_list')
        }
        Schedule = AsyncIOScheduler(jobstores=jobstores)
        Schedule.start()
        logger.info("Created Schedule Object")   
    except:    
        logger.error("Unable to Create Schedule Object")       


@app.on_event("shutdown")
async def pickle_schedule():
    """
    An Attempt at Shutting down the schedule to avoid orphan jobs
    """
    global Schedule
    Schedule.shutdown()
    logger.info("Disabled Schedule")


class AddJob(BaseModel):
    job_type: Literal["date", "cron", "interval"]
    interval: int
    
class RemoveJob(BaseModel):
    job_id : str


def task1():
    """ 
    Defining a task1
    """
    start = time.process_time()
    print("Executing prompt1 Task1...")
    time.sleep(2)
    print(time.process_time() - start)
    return 1

def task2():
    """ 
    Defining a task2
    """
    print('Task2 Started..')
    for i in range(0,9999999999999999999999):
        continue
    return i


@app.get("/schedules/")
async def get_jobs():
    """
    Get a list of jobs from a Schedule
    """
    schedules = []
    for job in Schedule.get_jobs():
        schedules.append({"job_id": str(job.id), "run_frequency": str(job.trigger), "next_run": str(job.next_run_time)})
    return {"jobs":schedules}


@app.post("/schedules/add/")
async def add_jobs(add:AddJob):
    """
    Add a New Job to a Schedule
    """
    if add.job_type == 'date':
        Schedule.add_job(task1, 'date', run_date=datetime.datetime.now())
        
    if add.job_type == 'cron':
        Schedule.add_job(task1, 'cron', hour= 2, minute= '*')
    
    if add.job_type == 'interval':
        Schedule.add_job(task1, 'interval', seconds=15)
        
    return {"scheduled":True}


@app.delete("/schedules/remove/")
async def remove_jobs(job:RemoveJob):
    """ 
    Remove A Job In The Schedule
    """
    res = Schedule.remove_job(job.job_id)
    return {"removed": res}



if __name__ == "__main__":
    uvicorn.run("fastapi-ap:app", port=9000, reload=True)



