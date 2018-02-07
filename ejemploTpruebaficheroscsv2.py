import socket
import json
import time
import random
import pandas as pd

# Logstash TCP/JSON Host
JSON_PORT = 5959
JSON_HOST = 'localhost'


if __name__ == '__main__':
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)#se establece conexion
        s.connect((JSON_HOST, JSON_PORT))
	#Se abre el fichero .csv y se leen los datos en el orden que le pasamos a continuacion
	df= pd.read_csv('/home/osboxes/datospruebacsv2.csv', names=['carrera', 'dni', 'pulsometro', 'usuario', 'distancia_total', 'tiempo_total', 'ritmo', 'edad', 'poblacion', 'pulso_max_user', 'pulso_rep_user', 'temperatura','ritmo_seg'])
	#Se establecen algunas variables para realizar algunas operaciones
	pulsaciones_ant=9999
	pulsaciones_act=0
	alerta1=0
	alerta2=0
	temp_user=0
	distancia_ant=100
	ritmo_ant_seg=0
	dorsal= 729
	dorsal=int(dorsal)

	#se van introduciendo los valores del fichero en variables
	for index, row in df.iterrows():

		nombre_carrera = row['carrera']
		dni_user = row['dni']
		pulsaciones_act = int(row['pulsometro'])
		nombre_user = row['usuario']
		dist_total = float(row['distancia_total'])
		tiemp_total = row['tiempo_total']
		ritmo_act = row['ritmo']
		edad_user = int(row['edad'])
		poblacion_user = row['poblacion']
		pulso_max = int(row['pulso_max_user'])
		pulso_rep = int(row['pulso_rep_user'])
		temp_user = float(row['temperatura'])
		ritmo_act_seg = int(row['ritmo_seg'])	

	#se establece una serie de condiciones
		if pulsaciones_act > pulsaciones_ant *1.2:
			alerta1 = 1
		else:
			alerta1 = 0
		alerta1=int(alerta1)

					
		if temp_user > 37.2:
			alerta2 = 1
		else:
			alerta2 = 0
		if temp_user < 35.8:
                        alerta2=1
                else:
                        alerta2 = 0

		alerta2=int(alerta2)

		if dist_total == distancia_ant:
			alerta3 = 1
		else:
			alerta3 = 0
		alerta3=int(alerta3)

		if pulsaciones_act > pulso_max:
			alerta4 = 1
		else:
			alerta4 = 0
		
		if pulsaciones_act < pulso_rep:
			alerta5 = 1
		else:
			alerta5 = 0

		if ritmo_act_seg < ritmo_ant_seg - (30 * ritmo_ant_seg / 100): 
			alerta6 = 1
		else:
			alerta6 = 0

 
		alerta3=int(alerta3)
		alerta4=int(alerta4)
		alerta5=int(alerta5)
		alerta6=int(alerta6)
		
		ritmo_ant_seg = ritmo_act_seg
		distancia_ant = dist_total		
		pulsaciones_ant = pulsaciones_act
		# se crea el fichero JSON                
       		data = {'carrera': nombre_carrera,'dni': dni_user, 'pulsometro': pulsaciones_act, 'user': nombre_user, 'distancia_total': dist_total, 'tiempo_total': tiemp_total, 'ritmo': ritmo_act,'edad': edad_user, 'poblacion': poblacion_user, 'pulso_max_user': pulso_max, 'pulso_rep_user': pulso_rep, 'temperatura': temp_user, 'alerta_pulso2': alerta1, 'alerta_temp2': alerta2, 'alerta_distancia': alerta3, 'alerta_pulso_max': alerta4, 'alerta_pulso_rep': alerta5, 'alerta_ritmo': alerta6, 'ritmo_seg': ritmo_act_seg, 'dorsal_corredor':dorsal, 'hostname':socket.gethostname() }


            	data_string = json.dumps(data)
	    	s.send(json.dumps(data)) # se envia el fichero
	    	print 'JSON: ',data_string

	    	s.send('\n')
            	time.sleep(5)# se repite el proceso cada 10 segundos

    # interrupt
    except KeyboardInterrupt:
        print("Program interrupted")
