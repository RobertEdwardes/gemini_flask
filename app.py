from flask import Flask
from flask import render_template
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler

import requests
import re
from bs4 import BeautifulSoup
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime, timedelta

nltk.download('vader_lexicon')
# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True
db = SQLAlchemy()
# create app
app = Flask(__name__)
app.config.from_object(Config())

# initialize scheduler
scheduler = APScheduler()

scheduler.init_app(app)
scheduler.start()

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///project.db"
db.init_app(app)

class lastScraped(db.Model):
    __tablename__ = 'sc_table'

    id = db.Column(db.Integer, primary_key=True)
    last_ran =db.Column(db.String)

class MyModel(db.Model):
    __tablename__ = 'my_table'

    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.JSON)
    url = db.Column(db.String)
    created_at = db.Column(db.DateTime, default=db.func.now())


with app.app_context():
    db.create_all()

@scheduler.task('interval', id='do_job_1', hours=1, misfire_grace_time=900)
def job1():
    with scheduler.app.app_context():
        subjects = ['car','music','housing','jobs','tech']
        for subject in subjects:
            url = f'https://www.google.com/search?q={subject}+news&num=10&sca_esv=593424282&tbm=nws&sxsrf=AM9HkKlpSy9BkLjnMpOE1ae8AgDnD54-3Q:1703425107388&source=lnms&sa=X&ved=0ahUKEwicpbWDmaiDAxW5v4kEHWbWB78Q_AUIBigB'
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            store_json = {}
            for i in soup.find_all(class_='Gx5Zad'):
                sid = SentimentIntensityAnalyzer()
                sent = []
                cleaned_url = re.sub(r'/url\?q=(.*?)', r'\1', i.a['href'])
                cleaned_url = re.sub(r'&sa.*', '', cleaned_url)
                
                for j in i.a:
                    if len(j.text) < 10:
                        continue
                    sent.append(j.text)
                if sent == []:
                    continue
                store_json['url'] = cleaned_url
                sent = ' '.join(sent)
                ss = sid.polarity_scores(sent)
                store_json['Sentance'] = sent
                for k in sorted(ss):
                    store_json[k] = ss[k]
                store_json['catagory'] = subject
                new_record = MyModel(url=cleaned_url ,data=store_json)
                db.session.add(new_record)
                db.session.commit()
                db.session.query(MyModel).filter(
                    MyModel.id.notin_(
                        db.session.query(db.func.min(MyModel.id)).group_by(MyModel.url).subquery()
                    )
                ).delete(synchronize_session=False)
            print(f'{subject} has been scraped')
            lastScraped.query.delete()
            db.session.commit()
            last_time = lastScraped(last_ran=datetime.now())
            db.session.add(last_time)
            db.session.commit()
    
@scheduler.task('interval', id='do_job_2', hours=2, misfire_grace_time=1900)
def job2():
    with scheduler.app.app_context():
        N_hours_ago = datetime.now() - timedelta(hours=46)
        db.session.query(MyModel).filter(MyModel.created_at < N_hours_ago).delete()
        db.session.commit()
        print('job2')

@app.route('/')
def index():
    result = db.session.query(MyModel).all()
    datetime = db.session.query(lastScraped).first()
    datetime = datetime.last_ran.split('.')[:1][0]
    output = [result[i].data for i in range(len(result))]
    return render_template('index.html', output=output, datetime=datetime)

if __name__ == '__main__':
    app.run()
    scheduler.add_job(job2, job1)