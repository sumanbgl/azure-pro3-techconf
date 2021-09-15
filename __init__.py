import logging
import os
import azure.functions as func
import psycopg2
from datetime import  datetime as dt
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    logging.info('Python ServiceBus queue trigger processed message: %s',
                 msg.get_body().decode('utf-8'))

    notification_id = int(msg.get_body().decode('utf-8'))
    conn = None
    try:
        conn = psycopg2.connect(host="mytechconfdbserver.postgres.database.azure.com",
                                database="techconfdb",
                                user="azureuser@mytechconfdbserver",
                                password="Welcome@123")

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')

        cur = conn.cursor()
        
        logging.info('type of notification id: %s', type(notification_id))
        notification = fetch_notification_by_id(notification_id, cur)
                    
        logging.info(notification)
       
        attendees = fetch_attendees(cur)
        for attendee in attendees:
            logging.info(attendee)
            subject = '{}: {}'.format(attendee.first_name, notification.subject)
            send_email(attendee.email, subject, notification.message)

        status = 'Notified {} attendees'.format(len(attendees))
        logging.info('Status : %s',status)        
        update_notification_status_completed_date(status, dt.utcnow(), notification_id, cur, conn)

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')


def fetch_notification_by_id(id, cur):
    logging.info('fetch_notification_by_id: id - %s', str(id))    
    cur.execute("SELECT MESSAGE, SUBJECT FROM NOTIFICATION WHERE ID = %s",(id,))
    row = cur.fetchone()
    notification = Notification(row[0], row[1])
    return notification
    # for row in rows:
    #     notification = Notification(row[0], row[1])
    #     notifications.append(notification)

    # return notifications

def fetch_attendees(cur):
    cur.execute('SELECT FIRST_NAME, LAST_NAME, EMAIL FROM ATTENDEE')
    rows = cur.fetchall()
    attendees = []

    for row in rows:
        attendee = Attendee(row[0], row[1], row[2])
        attendees.append(attendee)

    return  attendees

def update_notification_status_completed_date(status, completed_date, id, cur, conn):
    update_sql = 'UPDATE NOTIFICATION SET STATUS = %s, COMPLETED_DATE = %s WHERE ID = %s'
    cur.execute(update_sql, (status, completed_date, id,))
    conn.commit()
    logging.info("update successful")

def send_email(email, subject, body):
    send_grid_api_key = os.environ['SENDGRID_API_KEY']
    if send_grid_api_key:
        try:
            email_msg = Mail(
                from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
                to_emails=email,
                subject=subject,
                plain_text_content=body)

            email_client = SendGridAPIClient(send_grid_api_key)
            email_client.send(email_msg)
            logging.info("Successfully notified %s", email)
        except Exception as ex:
            logging.error(ex)            

    

class Attendee(object):
    def __init__(self, fname, lname, email):
        self.first_name = fname
        self.last_name = lname
        self.email = email
    
    def __str__(self):
        return f'Attendee Info: {self.last_name} , {self.first_name}, {self.email}'        

class Notification(object):
    def __init__(self, msg, sub):       
        self.message = msg        
        self.subject = sub

    def __str__(self):
        return f'Notification: {self.message} , {self.subject}'