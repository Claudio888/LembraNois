import psycopg2
import requests


from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from datetime import timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler


app = FastAPI()


def job():
    print('Job running...')
    con = psycopg2.connect(host='localhost', database='lembraNoisDB', user='postgres', password='root')
    cur = con.cursor()
    cur.execute('select * from agenda')
    results = cur.fetchall()
    for result_index, result in enumerate(results):
        result_unique = results[result_index]
        print('fazendo o for', result_index)
        actualHourPlus30 = datetime.now() + timedelta(minutes=30)
        actualHour = datetime.now()
        print('hora atual + 30 min',actualHourPlus30)
        print('querie',result_unique[1])
        print('hora atual', actualHour)
        if actualHour <= result_unique[1] and result_unique[1] <= actualHourPlus30 :
            print('entrou no if')
            telegram_bot_sendtext("You have an appointment scheduled up to: {0} "
                                  "with the following reminder: {1}".format(result_unique[1].replace(microsecond=0), result_unique[2]))
    print('Job finished...')


def telegram_bot_sendtext(bot_message):
    bot_token = '723531138:AAEoUdc2-EZyyuDtVSf0SoN7nNgLbK6-8gQ'
    bot_chatID = '463595290'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    print('enviando mensagem para o telegram')
    response = requests.get(send_text)

    return {'Reminder sent with success': response.json()}


@app.get("/list/")
def read_root():
    con = psycopg2.connect(host='localhost', database='lembraNoisDB', user='postgres', password='root')
    cur = con.cursor()
    cur.execute('select * from agenda')
    recset = cur.fetchall()
    return {"Result": recset}
    con.close()


class Agenda(BaseModel):
    dayAndHour_ts: datetime
    appointment: str


@app.post("/insert/")
async def create_client(agenda: Agenda):
    con = psycopg2.connect(host='localhost', database='lembraNoisDB', user='postgres', password='root')
    cur = con.cursor()
    sql = "insert into agenda (dayAndHour, appointment) values ('{0}','{1}')".format(agenda.dayAndHour_ts, agenda.appointment)
    cur.execute(sql)
    con.commit()
    con.close()
    return agenda


scheduler = AsyncIOScheduler()
scheduler.add_job(job, 'interval', seconds=15)
scheduler.start()
