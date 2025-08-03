import sys
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.common.exceptions import TimeoutException
import pytz
import logging
import requests
import os
import io
import time
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from  psycopg2 import connect, DatabaseError
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import json
class conext():
    def ConexionBdD(self):       
        data = self.parametrosBd() 
        conn = connect(host = data["host"] , port= data["port"], dbname= data["dbname"], user = data["user"], password= data["password"])
        conn.autocommit = True
        return conn

    def parametrosBd(self):
        datos = {
            "host": "bdrioclarosolutionwebdicprer2.cadtnfqjbvmi.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "dbname": "bdrioclarosolutionwebdicpreR2",
            "user": "rioClaroMaster",
            "password": "RioJu4n123Loz4n0"
        }
        return datos 

    def consultaAll(self, conn, sql):
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows


class controlMCode():

    def __init__(self,  baseparametrica = '', usuarioD = '', passwordD = '', proceso = 1, tipo = '', fechainicial = '', fechafinal = ''):
        super().__init__()
        self.usuarioD  = usuarioD
        self.passwordD = passwordD
        
        self.fechainicial = fechainicial
        self.fechafinal = fechafinal
        self.proceso = proceso
        # self.rutainicial = rutainicial
        self.baseparametrica = baseparametrica         
        self.now = datetime.now().strftime('%Y%m%d %H_%M')
        self.nombrelog = str(self.now) + '.log'
        rutainicial = os.path.dirname(os.path.abspath(__file__)) 
        self.rutalog = os.path.join(rutainicial, self.nombrelog)
        self.cx = conext()
        self.options = Options()
        
        self.options.add_experimental_option("detach", True)
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument('--disable-extensions')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-infobars')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-browser-side-navigation')
        self.options.add_argument('--disable-gpu')
        
        ## hace que no sea visible el navegador
        # self.options.add_argument('--headless=new')
 
        self.tipo = tipo
        self.driverCartera=webdriver.Chrome(self.options)
        # self.driverCartera.maximize_window()
        
        self.driverCartera.implicitly_wait(120)
        
        self.wait = WebDriverWait(self.driverCartera, 160)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(processName)s - %(message)s',
            filemode='a', filename=self.nombrelog)
        logging.debug('inicio')
        
        # Abre una nueva pestaña
        self.driverCartera.execute_script("window.open('','_blank');")
        # Abre una nueva pestaña
        self.driverCartera.execute_script("window.open('','_blank');")        
        # Cambia el control del driver a la nueva pestaña
        self.driverCartera.switch_to.window(self.driverCartera.window_handles[-2])
        self.secciones = ['Status','Start Time','End time','Run Duration']
        self.df = pd.DataFrame(columns=self.secciones)
        self.df.insert(0, "jobname", '')        
        self.CLASS_MAILITEM = 43
        self.MAILBOX_NAME = "recepcionfacturas@rioclaro.com.co"
        self.SENDER_NAME = 'Token Acceso DIAN'        
        self.nitEmpresa = ['900311157', '890927624']
        self.intentar = False
        self.ENV_AWS_ACCESS_KEY_ID, self.ENV_AWS_SECRET_ACCESS_KEY = self.get_aws_credentials_from_secret("DianWebScrapingFacturas")

            

    def descargar_y_procesar_zip_en_memoria(self, zip_url):
        # Descarga el archivo ZIP en memoria
        response = requests.get(zip_url)
        zip_bytes = io.BytesIO(response.content)
        # Abre el ZIP en memoria
        with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                    with zip_ref.open(file_name) as excel_file:
                        df = pd.read_excel(excel_file)
                        # Procesa el DataFrame aquí
                        # print(df.head())
                        # Puedes retornar el DataFrame o lo que necesites
                        return df
    def descargar_zip_en_memoria(self, zip_url):
        response = requests.get(zip_url)
        zip_bytes = io.BytesIO(response.content)
        return zip_bytes  # Esto es el ZIP en memoria

    def is_element_present(self, how, what):
        try:
            self.driverCartera.find_element(by=how, value=what) # to find page elements
            return True
        except Exception as error:
            return False

                
    def is_alert_present(self):
        try:
            time.sleep(1)
            self.driverCartera.switch_to.alert 
            return True
        except Exception as error:
            return False   
     
        
    def logueoControlm(self, usuario,password ):
        self.driverCartera.find_element(By.XPATH, '//*[@id="login-user-name"]').send_keys(usuario)
        self.driverCartera.find_element(By.XPATH, '//*[@id="login-user-password"]').send_keys(password)
        self.driverCartera.find_element(By.XPATH, '//*[@id="login-submit-button"]').click()
        time.sleep(3)
    
    def logueoAlm(self, usuario,password ):
        while self.is_element_present(By.XPATH,'//*[@id="legalRepresentative"]') == False:
            time.sleep(0.6)           
            # time.sleep(1)
        time.sleep(5)
        self.driverCartera.find_element(By.XPATH, '//*[@id="legalRepresentative"]').click()
        time.sleep(1)
        while self.is_element_present(By.XPATH,'//*[@id="UserCode"]') == False:
            time.sleep(0.6)  
        self.driverCartera.find_element(By.XPATH, '//*[@id="UserCode"]').send_keys(usuario)
        
        self.driverCartera.find_element(By.XPATH, '//*[@id="CompanyCode"]').send_keys(password)
        
        self.driverCartera.find_element(By.XPATH, '//*[@id="form0"]/button').click()
        
        while self.is_element_present(By.XPATH,'/html/body/div[3]/div/p') == False:
            if self.driverCartera.find_element(By.XPATH, '//*[@id="errorModal-title"]').text == "Captcha inválido":
                self.intentar = True
                return 
                # break
            time.sleep(0.6) 
        time.sleep(10)
        

    def recorreralmanaque(self,rows, diabuscar):
        # Recorre cada fila
        for row in rows:
            # Encuentra todas las celdas dentro de la fila actual
            cells = row.find_elements(By.XPATH, './/td')
            for cell in cells:
                # Intenta encontrar un elemento span con la clase disabled dentro de la celda, esto por que solo deja seleccionar los que no tienen la clase disabled
                disabled_spans = cell.find_elements(By.XPATH, './/span[contains(@class, "disabled")]')
                # Si no se encontró ningún span con la clase disabled, imprime el texto de la celda
                if not disabled_spans:
                    if cell.text == str(diabuscar):
                        cell.click()
                        return

    def EsperarsiEncuentraDatos(self,):
        time.sleep(3)
        while self.is_element_present(By.XPATH,'//*[@id="jobSearchResultGrid"]/div/ag-grid-angular/div/div[2]/div[2]/div[3]/div[2]/div/div' ) == False:
            time.sleep(1) 
            if self.is_element_present(By.XPATH, '/html/body/app-app-main/div/div/div/monitoring-domain/div[2]/job-search-result/div/div[3]/as-split/as-split-area[1]/div') == True:
                noencontrado = self.driverCartera.find_element(By.XPATH, '/html/body/app-app-main/div/div/div/monitoring-domain/div[2]/job-search-result/div/div[3]/as-split/as-split-area[1]/div').text
                if noencontrado in ' No Results Found ':                
                    return False
                
        return True 
             
    def pywin32_to_datetime(self, pywin32_datetime):
        # Crear una fecha y hora en la zona horaria UTC
        utc_datetime = datetime(
            year=pywin32_datetime.year,
            month=pywin32_datetime.month,
            day=pywin32_datetime.day,
            hour=pywin32_datetime.hour,
            minute=pywin32_datetime.minute,
            second=pywin32_datetime.second,
            tzinfo=pytz.utc  # Agregar información de zona horaria UTC
        )
        return utc_datetime
    
    def wait_for_page_load(self, timeout=90):
        self.wait.until(lambda _: self.driverCartera.execute_script('return document.readyState') == 'complete')

    def wait_for_element(self,  xpath, timeout=90):
        self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)) )

    def refresh_and_wait(self):
        self.driverCartera.refresh()
        self.wait_for_page_load(self)
        time.sleep(1)

    def get_date_text(self):
        return self.driverCartera.find_element(By.XPATH, "//div[@id='tableExport_wrapper']/table[@id='tableExport']/tbody/tr[@class='odd'][1]/td[1]").text

    def get_tooltip_text(self):
        tooltip = self.driverCartera.find_element(By.XPATH, "//table[@id='tableExport']/tbody/tr[@class='odd'][1]/td[@class='text-left'][2]/i")
        return tooltip.get_attribute('data-original-title')

    def validarDatosdiferentes(self, arrayantes):
        self.refresh_and_wait()
        self.wait_for_element( "//input[@id='ReceiverCode']")

        registro1Fecha = self.get_date_text()
        while registro1Fecha != datetime.now().strftime('%d-%m-%Y'):
            self.refresh_and_wait()
            self.wait_for_element( "//input[@id='ReceiverCode']")
            registro1Fecha = self.get_date_text()

        inner_html = self.get_tooltip_text()
        while inner_html != 'Listo':
            time.sleep(0.6)
            self.refresh_and_wait()
            inner_html = self.get_tooltip_text()
            
            
        registros = self.driverCartera.find_elements(By.XPATH, "//div[@id='tableExport_wrapper']/table[@id='tableExport']/tbody/tr")
        self.wait_for_tooltip()
        arrayDespues = self.get_text_elements(registros)
        if self.arrays_are_equal(arrayantes, arrayDespues):
            return True
        else:
            return False
   
    def wait_for_tooltip(self, text='Listo', interval=0.6):
        inner_html = self.get_tooltip_text()
        while inner_html != text:
            time.sleep(interval)
            inner_html = self.get_tooltip_text()

    def get_text_elements(self, registros):
        arrayAntes = []
        for registro in registros:
            elementos = registro.find_elements(By.XPATH, ".//td[@class='text-left'][1]")
            for elem in elementos:
                print(elem.text)
                arrayAntes.append(elem.text)
        return arrayAntes        
        
    def arrays_are_equal(self, array1, array2):
        # Si los arrays tienen longitudes diferentes, no pueden ser iguales
        if len(array1) != len(array2):
            return False

        # Compara los arrays posición por posición
        for i in range(len(array1)):
            if array1[i] != array2[i]:
                # Si encuentra una diferencia, devuelve False
                return False

        # Si no encontró ninguna diferencia, devuelve True
        return True


    ### metodos del correo fin 
    
    ############################################################################################################
    ############################################################################################################
    ############################################################################################################

    def get_aws_credentials_from_secret(secret_name, region_name="us-east-1"):
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        return secret_dict['ENV_AWS_ACCESS_KEY_ID'], secret_dict['ENV_AWS_SECRET_ACCESS_KEY']

    # # Uso:
    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = get_aws_credentials_from_secret("dian/aws/credentials")

    def enviar_correoAwsSes(self, asunto, adjuntos=None, concopia=None, nombre_adjunto='archivo.zip'):
        destinatario = 'recepcionfacturas@rioclaro.com.co'
        body_html = '<p>Se ha generado el archivo de facturas de la dian </p>'
        body_text = 'Se ha generado el archivo de facturas de la dian'
        ENV_AWS_ACCESS_KEY_ID = self.ENV_AWS_ACCESS_KEY_ID
        ENV_AWS_SECRET_ACCESS_KEY = self.ENV_AWS_SECRET_ACCESS_KEY
        AWS_REGION = "us-east-1"
        CHARSET = "utf-8"
        destinatario = destinatario.split(";")

        client = boto3.client('ses', region_name=AWS_REGION,
            aws_access_key_id=ENV_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=ENV_AWS_SECRET_ACCESS_KEY)
        msg = MIMEMultipart('mixed')
        msg['Subject'] = asunto
        msg['From'] = "no-responder@rioclarosolution.com"
        msg['To'] = ', '.join(destinatario)
        msg_body = MIMEMultipart('alternative')
        textpart = MIMEText(body_text.encode(CHARSET), 'plain', CHARSET)
        htmlpart = MIMEText(body_html.encode(CHARSET), 'html', CHARSET)
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        msg.attach(msg_body)

        # Adjuntar archivos
        if adjuntos:
            if isinstance(adjuntos, list):
                for path in adjuntos:
                    with open(path, 'rb') as f:
                        part = MIMEApplication(f.read())
                        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
                        msg.attach(part)
            elif hasattr(adjuntos, 'getvalue'):
                part = MIMEApplication(adjuntos.getvalue())
                part.add_header('Content-Disposition', 'attachment', filename=nombre_adjunto)
                msg.attach(part)

        try:
            response = client.send_raw_email(
                Source="no-responder@rioclarosolution.com",
                Destinations=[', '.join(destinatario)],
                RawMessage={'Data': msg.as_string()}
            )
            print("Email sent! Message ID:", response['MessageId'])
        except BotoCoreError as e:
            print(e)
            return e
        except ClientError as e:
            print(e)
            return e

    ############################################################################################################
    ############################################################################################################
    ############################################################################################################    
    # def webAlm(self, url, fechainicial, fechafinal, now ):
    def webAlm(self, url, fechainicial, fechafinal, now ):
        try:
            url = url
            # token = 'https://catalogo-vpfe.dian.gov.co/User/CompanyLogin'
            self.driverCartera.get(url)
            time.sleep(7)
         
            
            # while self.is_element_present(By.XPATH, '//*[@id="user-info-wrapper"]/p[@class="title"]') == False:
            #     time.sleep(0.6)
            #     print('esperando que cargue la pagina de la dian')
            WebDriverWait(self.driverCartera, 90).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="user-info-wrapper"]/p[@class="title"]'))
            )
            print('Página de la DIAN cargada')
            time.sleep(0.3)
            # element = self.driverCartera.find_element(By.XPATH, "//div[@id='user-info-wrapper']/p[@class='title']")

            #navega a la direccion de la dian para descargar la factura
            self.driverCartera.get("https://catalogo-vpfe.dian.gov.co/Document/Export")
            time.sleep(1)
            # while self.is_element_present(By.XPATH, "//input[@id='ReceiverCode']") == False:
            #     time.sleep(0.6)
            WebDriverWait(self.driverCartera, 90).until(
                EC.presence_of_element_located((By.XPATH, "//input[@id='ReceiverCode']"))
            )
            time.sleep(0.3)
            # self.driverCartera.find_element(By.XPATH, "//div[@class='row']/div[@class='col-sm-3 padding-xs-0'][2]/div[@class='form-group']/div[@class='input-group double-calendar']/input[@id='export-range']").click()
            # fechainicialinput = self.driverCartera.find_element(By.XPATH, "//div[@class='daterangepicker dropdown-menu show-calendar opensleft']/div[@class='calendar left']/div[@class='daterangepicker_input']/input[@class='input-mini active']")
            # fechainicialinput.clear()

            # # Antes de enviar a send_keys
            # fechainicial_str = fechainicial.strftime('%Y-%m-%d')
            # fechafinal_str = fechafinal.strftime('%Y-%m-%d')
            # fechainicialinput.send_keys(fechainicial_str) 
            self.driverCartera.find_element(By.XPATH, "//input[@id='export-range']").click()
            fechainicialinput = WebDriverWait(self.driverCartera, 90).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='calendar left']//input[@class='input-mini active']"))
            )
            
            fechainicialinput = self.driverCartera.find_element(By.XPATH, "//div[@class='daterangepicker dropdown-menu show-calendar opensleft']/div[@class='calendar left']/div[@class='daterangepicker_input']/input[@class='input-mini active']")
            fechainicialinput.clear()

            # Antes de enviar a send_keys
            fechainicial_str = fechainicial.strftime('%Y-%m-%d')
            fechafinal_str = fechafinal.strftime('%Y-%m-%d')

            fechainicialinput.send_keys(fechainicial_str) 
            
            
            fechainicialinput = self.driverCartera.find_element(By.XPATH, "//div[@class='daterangepicker dropdown-menu show-calendar opensleft']/div[@class='calendar right']/div[@class='daterangepicker_input']/input[@class='input-mini']")
            fechainicialinput.clear()
            # fechainicialinput.send_keys('2024-02-16')        
            fechainicialinput.send_keys(fechafinal_str)        
            
            
            
            dropdownlist = self.driverCartera.find_element(By.XPATH, "//button[@class='btn dropdown-toggle selectpicker btn-default']/span[@class='filter-option pull-left']")
            dropdownlist.click()
            
            # lilista = self.driverCartera.find_elements(By.XPATH, "//div[@class='row']/div[@class='col-sm-3 padding-xs-0'][3]/div[@class='form-group margin-bottom-0']/div[@class='btn-group bootstrap-select form-control open']/div[@class='dropdown-menu open']/ul[@id='ulfilter-groups']/li")
            # Recorre los elementos li
            
            lilista = WebDriverWait(self.driverCartera, 90).until(
                EC.presence_of_all_elements_located((By.XPATH, 
                                                     "//div[@class='row']/div[@class='col-sm-3 padding-xs-0'][3]/div[@class='form-group margin-bottom-0']/div[@class='btn-group bootstrap-select form-control open']/div[@class='dropdown-menu open']/ul[@id='ulfilter-groups']/li"
                                                     ))
            )
            # Recorre los elementos li
            for li in lilista:
                # Comprueba si el texto del elemento li es "Recibidos"
                if li.text == "Recibidos":
                    li.click()
                    break
            
            arrayAntes = []
            arrayDespues = []
            registros = self.driverCartera.find_elements(By.XPATH, "//div[@id='tableExport_wrapper']/table[@id='tableExport']/tbody/tr")
            self.wait_for_tooltip()
            arrayAntes = self.get_text_elements(registros)
            time.sleep(0.2)
            self.driverCartera.find_element(By.XPATH, "//button[@class='btn btn-success btn-radian-success btn-export-excel']").click()
            time.sleep(0.6)
            while self.is_element_present(By.XPATH, "//div[@class='modal-footer text-center']/button[@id='confirmModal-confirm-button']") == False:
                time.sleep(0.6)
            time.sleep(0.8)
            
            # da click en la ventana modal para confirmar la generacion de la consulta
            self.driverCartera.find_element(By.XPATH, "//div[@class='modal-footer text-center']/button[@id='confirmModal-confirm-button']").click()

            time.sleep(5)
            intentos = 0
            
            while self.validarDatosdiferentes(arrayAntes) == True:
                if intentos >= 20:
                    break
                time.sleep(4)
                intentos += 1

            # self.driverCartera.find_element(By.XPATH, "//table[@id='tableExport']/tbody/tr[@class='odd'][1]/td[@class='text-left'][3]/a[@class='btn btn-xs btn-hover-gosocket add-tooltip']/i[@class='fa fa-download']").click()
   
            # enlace_zip = self.driverCartera.find_element(
            #     By.XPATH, "//table[@id='tableExport']/tbody/tr[@class='odd'][1]/td[@class='text-left'][3]/a[@class='btn btn-xs btn-hover-gosocket add-tooltip']"
            # )

            # zip_url = enlace_zip.get_attribute('href')
            # print(f"URL del ZIP: {zip_url}")
            # Espera el enlace del ZIP
            enlace_zip = WebDriverWait(self.driverCartera, 90).until(
                EC.presence_of_element_located((By.XPATH, "//table[@id='tableExport']/tbody/tr[@class='odd'][1]/td[@class='text-left'][3]/a"))
            )
            zip_url = enlace_zip.get_attribute('href')
            print(f"URL del ZIP: {zip_url}")
            time.sleep(0.8)
            df = self.extraer_excel_de_zip_en_memoria(zip_url)
            time.sleep(0.220)
            # print(f"DataFrame extraído: {df.head()}")
            time.sleep(0.220)
            return df
        
        except Exception as error:                
            print(error)
            print('mirar si es aca ')
            tb = sys.exc_info()[2]
            tbinfo = str(traceback.format_tb(tb)[0]) 
            print(f"error en codigo, {tbinfo}")
            tbinfo = tbinfo.replace("'", "")
            tbinfo = str(tbinfo.replace('"', '')   )
            tbinfo =str(tbinfo) + str(error)  
            
    def extraer_excel_de_zip_en_memoria(self, zip_url):
        # Obtener cookies de Selenium y pasarlas a requests
        session = requests.Session()
        for cookie in self.driverCartera.get_cookies():
            session.cookies.set(cookie['name'], cookie['value'])

        response = session.get(zip_url)
        zip_bytes = io.BytesIO(response.content)
        try:
            with zipfile.ZipFile(zip_bytes, 'r') as archivo_zip:
                for nombre_archivo in archivo_zip.namelist():
                    if nombre_archivo.endswith('.xlsx') or nombre_archivo.endswith('.xls'):
                        with archivo_zip.open(nombre_archivo) as excel_file:
                            df = pd.read_excel(excel_file)
                            return df  # Retorna el primer Excel encontrado
        except zipfile.BadZipFile:
            print("El archivo descargado no es un ZIP válido. Probablemente es una página de error o login.")
            print(response.text)  # Para depuración, imprime el HTML recibido
        return None
    

    def waitChange(self):
        try:
            WebDriverWait(self.driverCartera, timeout=220).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
        except Exception as error:
            print("Error: %s" % error)
            logging.error("Error: %s" % error)
            print(error)
            tb = sys.exc_info()[2]
            tbinfo = str(traceback.format_tb(tb)[0]) 
            logging.error(f"error en waitChange {tbinfo}")
            
                 
    def descargarFacturas(self,df ):
        try:
            self.driverCartera.get(f"https://catalogo-vpfe.dian.gov.co/Document/Received")
            
            for index, row in df.iterrows():

                self.waitChange()
                # campocufe = self.driverCartera.find_element(By.XPATH, 
                #                                             "//div[@class='col-md-12 padding-horizontal-0']/div[@id='document-list']/div[@class='panel-body padding-20']/div[@id='divFirstRowDocFilter']/div[@class='col-sm-3 padding-xs-0'][1]/div[@class='form-group']/input[@id='DocumentKey']")
                self.waitChange()
                campocufe = WebDriverWait(self.driverCartera, 20).until(
                    EC.presence_of_element_located((By.XPATH, 
                    "//div[@class='col-md-12 padding-horizontal-0']/div[@id='document-list']/div[@class='panel-body padding-20']/div[@id='divFirstRowDocFilter']/div[@class='col-sm-3 padding-xs-0'][1]/div[@class='form-group']/input[@id='DocumentKey']"))
                )
                campocufe.clear()
                # time.sleep(1)
                campocufe.send_keys(row['CUFE/CUDE'])
                self.waitChange()
                time.sleep(0.2)
                ## da click en buscar por cufe
                # self.driverCartera.find_element(By.XPATH, "//div[@class='col-md-12 padding-horizontal-0']/div[@id='document-list']/div[@class='panel-footer-grey text-center submit-container']/button[@class='btn btn-success btn-radian-success']").click()
                # time.sleep(2)
                buscar_btn = WebDriverWait(self.driverCartera, 50).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@class='col-md-12 padding-horizontal-0']/div[@id='document-list']/div[@class='panel-footer-grey text-center submit-container']/button[@class='btn btn-success btn-radian-success']"))
                )
                buscar_btn.click()
                
                
                prefijo = row['Prefijo']
                if pd.isnull(prefijo) or prefijo == '':
                    prefijo = row['Folio']
                else:
                    prefijo = f"{row['Prefijo']}{row['Folio']}"
                self.waitChange()
                time.sleep(0.2)
                
                WebDriverWait(self.driverCartera, 90).until(
                        EC.text_to_be_present_in_element(
                            (By.XPATH, "//table[@id='tableDocuments']/tbody/tr[@class='odd document-row']/td[5]"),
                            str(row['Prefijo']) if pd.notnull(row['Prefijo']) and row['Prefijo'] != '' else str(row['Folio'])
                        )
                    )
                

                    
                self.waitChange()
                time.sleep(0.4)
                
                self.waitChange()
                # print(self.driverCartera.page_source)
                print()

                time.sleep(0.2)
                
                
                cufe = row['CUFE/CUDE']
                # self.driverCartera.find_element(By.XPATH, f"//div[@id='tableDocuments_wrapper']/table[@id='tableDocuments']/tbody/tr[@class='odd document-row']/td[@class='no-navigate sorting_1']/button[@id='{row['CUFE/CUDE']}']").click()

                downloadpdf = WebDriverWait(self.driverCartera, 90).until(
                    EC.element_to_be_clickable((By.XPATH, f"//div[@id='tableDocuments_wrapper']/table[@id='tableDocuments']/tbody/tr[@class='odd document-row']/td[@class='no-navigate sorting_1']/button[@id='{row['CUFE/CUDE']}']"))
                )
                downloadpdf.click()
                
                zip_url = f"https://catalogo-vpfe.dian.gov.co/Document/DownloadZipFiles?trackId={cufe}"
                session = requests.Session()
                for cookie in self.driverCartera.get_cookies():
                    session.cookies.set(cookie['name'], cookie['value'])
                response = session.get(zip_url)
                zip_bytes = io.BytesIO(response.content)

                asunto = f"{row['NIT Emisor']};{row['Nombre Emisor']};{prefijo};01;{row['Nombre Emisor']};"
                print(f"asunto es {asunto}")
                self.enviar_correoAwsSes(asunto, zip_bytes, nombre_adjunto= prefijo)

                self.waitChange()
        except Exception as error:                
            print(error)
            print('mirar si es aca ')
            tb = sys.exc_info()[2]
            tbinfo = str(traceback.format_tb(tb)[0]) 
            print(f"error en codigo, {tbinfo}")
            tbinfo = tbinfo.replace("'", "")
            tbinfo = str(tbinfo.replace('"', '')   )
            tbinfo =str(tbinfo) + str(error)   

    def EjecutarWeb(self, token, fechainicial, fechafinal, correo = "recepcionfacturas@rioclaro.com.co"):
        try:
            print("ebntro a la web ")
            # urlalm = 'https://catalogo-vpfe.dian.gov.co/User/CompanyLogin'
       
            # Crear una zona horaria para Colombia
            colombia_tz = pytz.timezone('America/Bogota')

            # Obtener la hora actual en la zona horaria de Colombia
            now = datetime.now(colombia_tz)

            print(f"fecha inicial es {fechainicial} y la fecha final es {fechafinal}")
            fecha_inicial = datetime.strptime(fechainicial, '%Y-%m-%d')
            fechafinal = datetime.strptime(fechafinal, '%Y-%m-%d')
            print(f"fecha inicial es {fecha_inicial} y la fecha final es {fechafinal}")
            
            df = self.webAlm(token, fechainicial= fecha_inicial, fechafinal= fechafinal, now= now)

            time.sleep(2)
            fechafinalinicial = fechafinal.strftime('%Y-%m-%d')
            
            fecha_inicial = fecha_inicial - timedelta(days=10)
            fecha_inicial = fecha_inicial.strftime('%Y-%m-%d')
            
            fechafinal = fechafinal #+ timedelta(days=10)
            fechafinal = fechafinal.strftime('%Y-%m-%d')
        
                            
            self.fechainicial = fecha_inicial
            self.fechafinal = fechafinal
            # # --------------------------------------------------------------------------------------------
            conn =self.cx.ConexionBdD()
            sql = f"""select  * from pos_factura where "FechaEmisionFactura" between '{self.fechainicial}' and '{self.fechafinal}'"""
            dfb = pd.read_sql_query(con=conn, sql= sql)
            
            fechainicial3 = pd.to_datetime(fechainicial, format= '%Y-%m-%d')
            fechafinal3 = pd.to_datetime(fechafinal, format= '%Y-%m-%d')

            df['Fecha Emisión'] = pd.to_datetime(df['Fecha Emisión'], format='%d-%m-%Y')
            df = df[(df['Fecha Emisión'] >= fechainicial3) & (df['Fecha Emisión'] <= fechafinal3)]

            df = df[(df['Tipo de documento'] != 'Application response')]
            df['NIT Receptor'] = df['NIT Receptor'].astype(str)
            df = df[(df['NIT Receptor'].isin(self.nitEmpresa))]
            
            df["FacturaCompleta"] = df.apply(lambda row: row['Folio'] if pd.isna(row['Prefijo']) or row['Prefijo'] == '' else row['Prefijo'] + row['Folio'], axis=1)

            cufe_a_no_en_b = df[~df['CUFE/CUDE'].isin(dfb['cufe'])]
            cufe_a_no_en_b  = cufe_a_no_en_b.reset_index(drop=True)
            
            cufe_a_no_en_b.to_excel("cufe_a_no_en_b.xlsx", index=False)

            if cufe_a_no_en_b.shape[0] > 0:
                self.descargarFacturas(cufe_a_no_en_b)
            
            # self.setCurrentProgress.emit(13)            
            logging.info('Proceso terminado')
        
            
        except Exception as error:                
            print(error)
            print('mirar si es aca ')
            tb = sys.exc_info()[2]
            tbinfo = str(traceback.format_tb(tb)[0]) 
            print(f"error en codigo, {tbinfo}")
            tbinfo = tbinfo.replace("'", "")
            tbinfo = str(tbinfo.replace('"', '')   )
            tbinfo =str(tbinfo) + str(error)   
            logging.error(f"Error en EjecutarWeb: {tbinfo}")
        finally:
            try:
                self.driverCartera.quit()
            except Exception as e:
                print(f"Error cerrando el driver: {e}")
                logging.error(f"Error cerrando el driver: {e}")
            logging.info('Driver cerrado correctamente')
            print('Driver cerrado correctamente')

