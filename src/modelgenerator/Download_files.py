"""
Module: Download_files
=============================

A module that downloads data that is required for the GEOSeMOSYS analysis and unzips them and places them in a new folder
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""

from urllib.request import Request, urlopen
import shutil
import zipfile
from gzip import open as gzopen
import tarfile
import pandas as pd
import requests
import json
import os
from tkinter import *

# Retrieve access token
def download_viirs(url_viirs, temp):
    """
    This function downloads the night time light data from EOG and places it in param temp. It requires a login to that webpage which is free.
    :param url_viirs:
    :param temp:
    :return:
    """
    def get_input():
        pwd= entry1.get()
        uname = entry2.get()

        params = {
            'client_id': 'eogdata_oidc',
            'client_secret': '2677ad81-521b-4869-8480-6d05b9e57d48',
            'username': uname,
            'password': pwd,
            'grant_type': 'password'
        }
        token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
        response = requests.post(token_url, data=params)
        master.destroy()
        access_token_dict = json.loads(response.text)
        access_token = access_token_dict.get('access_token')
        # Submit request with token bearer
        ## Change data_url variable to the file you want to download
        data_url = url_viirs
        auth = 'Bearer ' + access_token
        headers = {'Authorization': auth}
        response = requests.get(data_url, headers=headers)
        # Write response to output file
        output_file = "../%s/%s" % (temp, 'VNL_v2_npp_2020_global_vcmslcfg_c202102150000.average_masked.tif.gz')
        with open(output_file, 'wb') as f:
            f.write(response.content)

    master = Tk()

    label1 = Label(master, text="Username Eart Observation group")
    label1.pack()
    label1.config(justify=CENTER)
    label2 = Label(master, text="Password Eart Observation group", width=30)
    label2.pack()
    label2.config(justify=CENTER)

    entry1 = Entry(master)
    entry2 = Entry(master)

    button = Button(master, text="Submit")
    button.config()

    entry2 = Entry(master, width=30)
    entry2.pack()
    entry1 = Entry(master, width=30)
    entry1.pack()

    buttone1 = Button(master, text="Submit")
    buttone1.pack()
    buttone1.config(command = get_input)

    master.mainloop()



def download_url_data(url,temp):
    """ This function downloads the data from URL in url (comma separated file) and place them in temp folder.

    :param url:
    :param temp:
    :return:
    """

    def create_dir(dir):
        if not os.path.exists(dir):
            os.makedirs(dir)
    create_dir(('../' + temp))
    url_adress = pd.read_csv(url, header=None, sep=',')
    for i, row in url_adress.iterrows():
        try:
            req = Request(row[0], headers={'User-Agent': 'Chrome'})
            with urlopen(req) as response, open("../%s/%s" % (temp, row[1]), 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        except Exception as e:
            print(e)
    return()


def unzip_all(url, fromfolder, tofolder):

    """ This function unzips the data from URL (url) in "fromfolder" and place them in  "tofolder".

    :param url:
    :param fromfolder:
    :param tofolder:
    :return:
    """

    def create_dir(dir):
        if not os.path.exists(dir):
            os.makedirs(dir)

    create_dir((tofolder))

    url_adress = pd.read_csv(url, header=None, sep=',')
    def unzip(infolder, outfolder):
        with zipfile.ZipFile(infolder, 'r') as zip_ref:
            zip_ref.extractall(outfolder)
        return ()

    def extract(tar_url, out_path):
        tar = tarfile.open(tar_url, 'r')
        for item in tar:
            tar.extract(item, out_path)
            if item.name.find(".tgz") != -1 or item.name.find(".tar") != -1:

                extract(item.name, "./" + item.name[:item.name.rfind('/')])
    def gzip_e(fin, fou):
        with gzopen(fin, 'rb') as s_file, \
                open(fou, 'wb') as d_file:
            shutil.copyfileobj(s_file, d_file)


    for i, row in url_adress.iterrows():
        _, filename = os.path.split(row[1])
        name, ending = os.path.splitext(filename)
        if ending == '.zip':
            unzip(os.path.join(fromfolder, row[1]), os.path.join(tofolder, name))
        elif ending == '.gz':
            gzip_e(os.path.join(fromfolder, row[1]), os.path.join(tofolder, name))
        else:
            shutil.copy(os.path.join(fromfolder, row[1]), os.path.join(tofolder, row[1]))


