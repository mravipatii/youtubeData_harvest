import googleapiclient.discovery
import pandas as pd
import streamlit as st
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from streamlit_option_menu import option_menu

import questiond, homeb, channelc
st.set_page_config(page_title="youtube data harvest")

class multiApp:
    def __init__(self):
        self.apps = []
    def add_app(self,title,function):
        self.apps.append({
            "title": title,
            "function": function
            })
    def run():
        with st.sidebar:
            app = option_menu(
                menu_title='youtube data',
                options=['home','channel','question'],
                #icons=[],
                #menu_icon='',
                default_index=0
                #styles={}
                )
        if app == 'channel':
            channelc.app()
        if app == 'question':
            questiond.app()
        if app == 'home':
            homeb.app()
    run()

            

