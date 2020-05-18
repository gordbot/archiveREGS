#!/usr/bin/python3

import csv
import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime

x = datetime.datetime.now()
db = x.strftime("%Y-%m-%d-%H-%M-%S") + '.sqlite3'
con = sqlite3.connect(db)
cur = con.cursor()
searchParlSessions = [[42,1],[41,2],[41,1],[40,3],[40,2],[40,1],[39,2],[39,1],[38,1]]

#Create Tables
def createTables():
    notice_table = """
    CREATE TABLE notice (
        parliament integer,
        session integer,
        meeting_date date,
        link text NOT NULL,
        notice text,
        PRIMARY KEY (parliament, session, meeting_date))"""
    cur.execute(notice_table)


    evidence_table = """
    CREATE TABLE evidence (
        parliament integer,
        session integer,
        meeting_date date,
        link text NOT NULL,
        evidence text,
        PRIMARY KEY (parliament, session, meeting_date))"""
    cur.execute(evidence_table)

    minutes_table = """
    CREATE TABLE minutes (
        parliament integer,
        session integer,
        meeting_date date,
        link text NOT NULL,
        minutes text,
        PRIMARY KEY (parliament, session, meeting_date))"""
    cur.execute(minutes_table)



# Links to Notice of Meeting
def getNoticeLinks(soup):
    notices = []
    noticelinks = soup.find_all('a', class_="btn-meeting-notice")
    for a in noticelinks:
        notices.append("https:" + a['href'])
    return notices

# Links to Meeting Evidence
def getEvidenceLinks(soup):
    evidence = []
    evidencelinks = soup.find_all('a', class_="btn-meeting-evidence")
    for a in evidencelinks:
        evidence.append("https:" + a['href'])
    return evidence

# Lnks to Meeting Minutes
def getMinuteLinks(soup):
    minutes = []
    minutelinks = soup.find_all('a', class_="btn-meeting-minutes")
    for a in minutelinks:
        minutes.append("https:" + a['href'])
    return minutes

#Function to extract meeting # & notice, and then insert those into sqlite notice table
def getNotices(parl, session, notices):
    for url in notices:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        meetingDate = soup.find("meta", attrs={'name':'MeetingDate'})['content']
        noticeText = soup.find(class_='NoticeBase').text
        #print("Meeting Date: ", meetingDate, "NoticeBase: ", noticeText)
        sql = """
        INSERT INTO notice (parliament, session, meeting_date, link, notice)
        VALUES (?, ?, ?, ?, ?)"""
        cur = con.cursor()
        cur.execute(sql, (parl, session, meetingDate, url, noticeText))


#Function to extract meeting # & evidence 
def getEvidence(parl, session, evidences):
    for url in evidences:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        meetingDate = soup.find("meta", attrs={'name':'MeetingDate'})['content']
        evidenceText = soup.find(class_='publication-container single-page').text
        #print("Meeting Date: ", meetingDate, "EvidenceText: ", evidenceText)
        sql = """
        INSERT INTO evidence (parliament, session, meeting_date, link, evidence)
        VALUES (?, ?, ?, ?, ?)"""
        cur = con.cursor()
        cur.execute(sql, (parl, session, meetingDate, url, evidenceText))

#Function to extract meeting # & evidence
def getMinutes(parl, session, notices):
    for url in notices:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        minutesText = soup.find(class_='publication-container single-page').text
        meetingDate = soup.find("meta", attrs={'name':'MeetingDate'})['content']
        #print("Meeting Date: ", meetingDate, "Minutes: ", minutesText)
        sql = """
        INSERT INTO minutes (parliament, session, meeting_date, link, minutes)
        VALUES (?, ?, ?, ?, ?)"""
        cur = con.cursor()
        cur.execute(sql, (parl, session, meetingDate, url, minutesText))

#Function to join meeting information together and create csv
def joinTablesAndExport():
        sql = """
        SELECT *
        FROM
        notice
        LEFT OUTER JOIN evidence USING (parliament, session, meeting_date)
        LEFT OUTER JOIN minutes USING (parliament, session, meeting_date)
        """
        cur = con.cursor()
        select = cur.execute(sql)
        #print(cur.fetchone())
        with open('output.csv', 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(['Parliament', 'Session', 'Meeting Date', 'Notice Text', 'Evidence Text', 'Minutes Text'])
            writer.writerows(select)

def main():
    # Create the tables
    createTables()
    # Get the index page
    for parl, session in searchParlSessions:
        url = "https://www.parl.ca/Committees/en/REGS/Meetings?parl=" + str(parl) + "&session=" + str(session)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')

        print("Parliament: ", parl, "Session: ", session)
        getNotices(parl,session,getNoticeLinks(soup))
        getEvidence(parl,session,getEvidenceLinks(soup))
        getMinutes(parl,session,getMinuteLinks(soup))
    #joinTablesAndExport()
    con.commit()
if __name__ == "__main__":
    main()
