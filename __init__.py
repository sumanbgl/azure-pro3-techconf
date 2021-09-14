import logging

import azure.functions as func
import psycopg2
from datetime import  datetime as dt

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
        
        notifications = fetch_notification_by_id(notification_id, cur)

        for notification in notifications:
            logging.info("{}, {}, {}, {}", notification.status, notification.message, notification.submitted_date, notification.subject)

        existing_notification = notifications[0]

        attendees = fetch_attendees(cur)
        for attendee in attendees:
            logging.info("{}, {}, {}", attendee.last_name, attendee.first_name, attendee.email)

        status = 'Notified {} attendees'.format(len(attendees))
        logging.info('Status : %s',status)
        insert_notification(existing_notification, status, cur, conn)

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')


def fetch_notification_by_id(id, cur):
    logging.info('fetch_notification_by_id')
    query = 'SELECT STATUS, MESSAGE, SUBMITTED_DATE, SUBJECT FROM NOTIFICATION WHERE ID=%s'
    # cur.execute('SELECT MESSAGE, SUBJECT FROM NOTIFICATION WHERE ID=3')
    cur.execute(query, (id,))
    rows = cur.fetchall()
    notifications = []

    for row in rows:
        notification = Notification(row[0], row[1], row[2], row[3])
        notifications.append(notification)

    return notifications

def fetch_attendees(cur):
    cur.execute('SELECT FIRST_NAME, LAST_NAME, EMAIL FROM ATTENDEE')
    rows = cur.fetchall()
    attendees = []

    for row in rows:
        attendee = Attendee(row[0], row[1], row[2])
        attendees.append(attendee)

    return  attendees

def insert_notification(existing_notification, status, cur, conn):
    insert_sql = 'INSERT INTO NOTIFICATION (STATUS, MESSAGE, SUBMITTED_DATE, COMPLETED_DATE, SUBJECT) VALUES (%s, %s, %s, %s, %s)'
    cur.execute(insert_sql, (status, existing_notification.message, existing_notification.submitted_date, str(dt.utcnow()), existing_notification.subject,))
    conn.commit()


class Attendee(object):
    def __init__(self, fname, lname, email):
        self.first_name = fname
        self.last_name = lname
        self.email = email

class Notification(object):
    def __init__(self, status, msg, submitted_date, sub):
        self.status = status
        self.message = msg
        self.submitted_date = submitted_date
        self.subject = sub
