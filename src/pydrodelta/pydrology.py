#/usr/bin/python3
#Librería de métodos para modelación hidrológica SSIyAH-INA, 2022
import math
from sys import maxsize
from zlib import MAX_WBITS
import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
import os, glob

#0. Definición de funciones (métodos transversales)

#Realiza Plot de Prueba entre Inflow y Outflow 
def testPlot(Inflow,Outflow):
    plt.plot(Inflow,'r')
    plt.plot(Outflow,'b')
    plt.show()

#Diferencia una serie
def differentiate(list):
    dif=[0]*len(list)
    for i in range(1,len(list)):
        dif[i]=list[i]-list[i-1]
    return dif

#Integra por método del trapecio
def integrate(list,dt):
    int=0
    for i in range(1,len(list)):
        int=int+(list[i]+list[i-1])*dt/2
    return int

#Computa Función Respuesta Unitaria Cascada de n reservorios Lineales con tiempo de residencia k, obtenida por integración numérica a resolución dt (método del trapecio)
def gammaDistribution(n,k,dt=1,m=10,round='T'):
    T=int(m*n*k)
    u=np.array([0]*(int(T/dt)+1),dtype='float')
    U=np.array([0]*(int(T)+1),dtype='float')
    j=0
    for t in np.array(list(range(0,int(T/dt)+1))):
        u[j]=1/(k*math.gamma(n))*(t*dt/k)**(n-1)*math.exp(-t*dt/k)
        j=j+1
    for j in range(1,int(T)+1):
        min=int((j-1)/dt)
        max=int(j/dt)+1
        U[j]=integrate(u[min:max],dt)
    if round == 'T':
        U=U/sum(U)
    return U

#Computa Matriz de pulsos para Convolución con función distribución (Sistemas Lineales). Inflow es el array 1D de inflows a propagar por un sistema lineal con función de transferencia representada por el array 1D u
def getPulseMatrix(inflows,u):
    n=len(inflows)
    m=len(u)
    rows=n+m-1
    I=np.array([[0]*m]*rows,dtype='float')
    k=0
    for col in range(0,int(m)):
        for row in range(0,int(rows)):
            if col>row:
                I[row,col]=0
            else:
                if row>=n+k:
                    I[row,col]=0
                else:
                    I[row,col]=inflows[row-k]
        k=k+1
    return(I)

#Computa Ecuación de Conservación
def waterBalance(Storage=0,Inflow=0,Outflow=0):
    Storage=Inflow-Outflow+Storage
    return Storage

#Computa EVR de acuerdo a las hipótesis de Thornthwaite
def computeEVR(P,EV0,Storage,MaxStorage):
    sigma=max(EV0-P,0)
    return(EV0+Storage*(1-math.exp(-sigma/MaxStorage))-sigma)

#Proratea Inflow
def apportion(Inflow,phi=0.1):
    return(Inflow*phi)

#Computa Runoff a partir del valor de Precipitación Efectiva Pe (acumulado en intervalo) y del almacenamiento a capacidad de campo o máximo almacenamiento
def curveNumberRunoff(NetRainfall,MaxStorage,Storage):
    return NetRainfall**2/(MaxStorage-Storage+NetRainfall)

#1. Proceso P-Q: Componentes de Función Producción de Escorrentía 

#1.A Reservorio de Retención 
class RetentionReservoir:
    """
    Reservorio de Retención. Vector pars de sólo parámetro: capacidad máxima de abstracción [MaxStorage]. Condiciones Iniciales (InitialConditions): [Initial Storage] (ingresa como vector). Condiciones de Borde (Boundaries): vectior [[Inflow],[EV]]. 
    """
    type='Retention Reservoir'
    def __init__(self,pars,InitialConditions=[0],Boundaries=[[0],[0]],Proc='Abstraction'):
        self.MaxStorage=pars[0]
        self.Inflow=np.array(Boundaries[0],dtype='float')
        self.EV=np.array(Boundaries[1],dtype='float')
        self.Storage=np.array([InitialConditions[0]]*(len(self.Inflow)+1),dtype='float')
        self.Runoff=np.array([0]*len(self.Inflow),dtype='float')
        self.Proc=Proc
        self.dt=1
    def computeRunoff(self):
        for i in range(0,len(self.Inflow),1):
            if self.Proc == 'Abstraction':
                self.Runoff[i]=max(0,self.Inflow[i]-self.EV[i]+self.Storage[i]-self.MaxStorage)
            if self.Proc == 'CN_h0_continuous':
                self.Runoff[i]=(max(self.Inflow[i]-self.EV[i],0))**2/(self.MaxStorage-self.Storage[i]+self.Inflow[i]-self.EV[i])
            self.Storage[i+1]=waterBalance(self.Storage[i],self.Inflow[i],self.EV[i]+self.Runoff[i])


#1.B Reservorio Lineal. 
class LinearReservoir:
    """
    Reservorio Lineal. Vector pars de un sólo parámetro: Tiempo de residencia (K). Vector de Condiciones Iniciales (InitialConditions): Storage, con el cual computa Outflow. Condiciones de Borde (Boundaries): Inflow y EV.
    """
    type='Linear Reservoir'
    def __init__(self,pars,InitialConditions=[0],Boundaries=[[0],[0]],Proc='Agg',dt=1):
        self.K=pars[0]
        self.Inflow=np.array(Boundaries[0],dtype='float') 
        self.EV=np.array(Boundaries[1],dtype='float')
        self.Storage=np.array([InitialConditions[0]]*(len(self.Inflow)+1),dtype='float')
        self.Outflow=(1/self.K)*self.Storage
        self.Proc=Proc
        if Proc == ('Agg' or 'API'):
            self.dt=1
        if Proc == 'Instant':
            self.dt=dt
    def computeOutFlow(self):
        for i in range (0,len(self.Inflow),1):
            if self.Proc == 'Agg':
                self.Outflow[i]=(1/self.K)*(self.Storage[i]+self.Inflow[i])
                self.Storage[i+1]=waterBalance(self.Storage[i],self.Inflow[i],self.Outflow[i])
            if self.Proc == 'Instant':
                end=int(1/self.dt+1)
                Storage=self.Storage[i]
                Outflow=self.Outflow[i]    
                for t in range(1,end,1):
                    Storage=waterBalance(Storage,self.Inflow[i]*self.dt,Outflow*self.dt)
                    Outflow=(1/self.K)*(Storage)
                self.Storage[i+1]=Storage
                self.Outflow[i+1]=Outflow
            if self.Proc == 'API':
                self.Storage[i+1]=(1-1/self.K)*self.Storage[i]+self.Inflow[i]
                self.Outflow[i+1]=(1/self.K)*self.Storage
                

class SCSReservoirs:
    """
    Sistema de 2 reservorios de retención (intercepción/abstracción superficial y retención en perfil de suelo - i.e. capacidad de campo-), con función de cómputo de escorrentía siguiendo el método propuesto por el Soil Conservation Service. Vector pars de dos parámetros: Máximo Almacenamiento Superficial (Abstraction) y Máximo Almacenamiento por Retención en Perfil de Suelo (MaxStorage). Condiciones iniciales: Almacenamiento Superficial y Almacenamiento en Perfil de Suelo (lista de valores). Condiciones de Borde: Hietograma (lista de valores).
    """
    type='Soil Conservation Service Model for Runoff Computation (Curve Number Method / Discrete Approach)'
    def __init__(self,pars,InitialConditions=[0,0],Boundaries=[0],Proc='Time Discrete Agg'):
        self.Abstraction=pars[0]
        self.MaxStorage=pars[1]
        self.Precipitation=np.array(Boundaries,dtype='float')
        self.SurfaceStorage=np.array([InitialConditions[0]]*(len(self.Precipitation)+1),dtype='float')
        self.SoilStorage=np.array([InitialConditions[1]]*(len(self.Precipitation)+1),dtype='float')
        self.Runoff=np.array([0]*len(self.Precipitation),dtype='float')
        self.Infiltration=np.array([0]*len(self.Precipitation),dtype='float')
        self.CumPrecip=np.array([0]*len(self.Precipitation),dtype='float')
        self.NetRainfall=np.array([0]*len(self.Precipitation),dtype='float') 
        self.Proc=Proc
        self.dt=1
    def computeAbstractionAndRunoff(self):
        for i in range(0,len(self.Precipitation)):
            if i == 0:
                  self.CumPrecip[i]=self.CumPrecip[i]+self.Precipitation[i]
            else:
                  self.CumPrecip[i]=self.CumPrecip[i-1]+self.Precipitation[i]
            if self.CumPrecip[i]-self.Abstraction > 0:
                   self.NetRainfall[i] = self.CumPrecip[i]-self.Abstraction
                   self.Runoff[i] = curveNumberRunoff(self.NetRainfall[i],self.MaxStorage,self.SoilStorage[0])
                   self.Infiltration[i]=self.NetRainfall[i]-self.Runoff[i]
            else:
                    self.NetRainfall[i] = 0
                    self.Runoff[i] = 0
            self.SurfaceStorage[i+1]=min(self.Abstraction,self.CumPrecip[i])
        self.Runoff=differentiate(self.Runoff)
        self.NetRainfall=differentiate(self.NetRainfall)
        self.Infiltration=differentiate(self.Infiltration)
        for i in range(0,len(self.SoilStorage)-1):
            self.SoilStorage[i+1]=waterBalance(self.SoilStorage[i],self.Infiltration[i])        

#2. Proceso Q-Q: Componentes de Función Distribución de Escorrentía o Tránsito Hidrológico

#2.A Cascada de Reservorios Lineales (Discreta). Dos parámetros: Tiempo de Resdiencia (K) y Número de Reservorios (N)
class LinearReservoirCascade:
    """
    Cascada de Reservorios Lineales (Discreta). Vector pars de dos parámetros: Tiempo de Residencia (K) y Número de Reservorios (N). Vector de Condiciones Iniciales (InitialConditions): Si es un escalar (debe ingresarse como elemento de lista) genera una matriz de 2xN con valor constante igual al escala, también puede ingresarse una matriz de 2XN que represente el caudal inicial en cada reservorio de la cascada. Condiciones de Borde (Boundaries): Inflow 
    """
    type='Discrete Cascade of N Linear Reservoirs with Time K'
    def __init__(self,pars,Boundaries=[0],InitialConditions=[0],create='yes',Proc='Discretely Coincident',dt=1):
        self.K=pars[0]
        if not pars[1]:
            self.N=2
        else:
            self.N=pars[1]
        self.Inflow=np.array(Boundaries)   
        if  create == 'yes':
            self.Cascade=np.array([[InitialConditions[0]]*self.N]*2,dtype='float')
        else:
            self.Cascade=np.array(InitialConditions,dtype='float')
        self.Outflow=np.array([InitialConditions[0]]*(len(Boundaries)+1),dtype='float')
        self.dt=dt
    def computeOutFlow(self):
        dt=self.dt
        k=self.K    
        c=math.exp(-dt/k)
        a=k/dt*(1-c)-c
        b=1-k/dt*(1-c)
        end=int(1/dt+1)
        for i in range(0,len(self.Inflow)):
            for n in range(1,end,1):
                self.Cascade[1][0]=self.Inflow[i]+(self.Cascade[0][0]-self.Inflow[i])*c
                if self.N > 1:
                    for j in range(1,self.N,1):
                        self.Cascade[1][j]=c*self.Cascade[0][j]+a*self.Cascade[0][j-1]+b*self.Cascade[1][j-1]
                for j in range(0,self.N,1):
                    self.Cascade[0][j]=self.Cascade[1][j]
            self.Outflow[i+1]=self.Cascade[0][j]

#2.B Canal Muskingum 
# EN DESARROLLO (MUSKINGUM y CUNGE) --> VER RESTRICCIONES NUMÉRICAS y SI CONSIDERAR CURVAS HQ y BH COMO PARAMETROS DEL METODO. POR AHORA FINALIZADO MUSKINGUM CLÁSICO. CUNGE DEBE APOYARSE SOBRE EL MISMO, MODIFICANDO PARS K y X
class MuskingumChannel:
    """
    Método de tránsito hidrológico de la Oficina del río Muskingum. Vector pars de dos parámetros: Tiempo de Tránsito (K) y Factor de forma (X) [Proc='Muskingum'] o . Condiciones Iniciales (InitialConditions): matriz de condiciones iniciales o valor escalar constante. Condiciones de borde: Hidrograma en nodo superior de tramo. 
    """
    #A fin de mantener condiciones de estabilidad numérica en la propagación (conservar volumen), sobre la base de la restricción 2KX<=dt<=2K(1-X) (Chin,2000) y como dt viene fijo por la condición de borde (e.g. por defecto 'una unidad') y además se pretende respetar el valor de K, se propone incrementar la resolución espacial dividiendo el tramo en N subtramos de igual longitud, con tiempo de residencia mínimo T=K/N, para el caso dt<2KX (frecuencia de muestreo demasiado alta). Luego, aplicando el criterio de chin se sabe que el valor crítico de dt debe satisfacer dt=uT, específicamente con u=2X y T = K/N--> N=2KX/dt. Al mismo tiempo si dt>2K(1-X) (frecuencia de muestreo demasiado baja), el paso de cálculo se subdivide en M subpasos de longitud dT=2K(1-X) de forma tal que dT/dt=dv y M=dt/dv. Self.tau especifica el subpaso de cálculo (siendo self.M la cantidad de subintervalos utilizados) y self.N la cantidad de subtramos. 
    type='Muskingum Channel'
    def __init__(self,pars,Boundaries=[0],InitialConditions=[0],Proc='Muskingum Routing Method',dt=1):
        self.K=pars[0]
        self.X=pars[1]
        self.dt=dt
        self.lowerbound=2*self.K*self.X
        self.upperbound=2*self.K*(1-self.X)
        self.Inflow=np.array(Boundaries,dtype='float')
        self.Outflow=np.array([0]*len(self.Inflow),dtype='float')
        self.InitialConditions=np.array(InitialConditions,dtype='float')
        self.N=1
        self.tau=self.dt
        if self.dt > self.upperbound:
            self.tau=self.upperbound
        else:
            if self.dt < self.lowerbound:
               self.N=round(self.lowerbound/self.dt) 
        self.M=round(self.dt/self.tau)
        if self.X > 1/2:
            raise NameError('X must be between 0 and 1/2')
        if len(InitialConditions) == 1:
            if InitialConditions[0] == 0:
                self.InitialConditions=np.array([[0]*(self.N+1)]*2,dtype='float')
            else:    
                self.InitialConditions=np.array([[InitialConditions[0]]*(self.N+1)]*2,dtype='float')
        if len(self.InitialConditions[0]) < self.N:
            raise NameError('Matrix of Initial Conditions must have'+str(self.N+1)+'cols as it have'+str(self.N)+'subreaches')        
        self.Outflow[0]=self.InitialConditions[1][self.N]
    def computeOutFlow(self):
        K=self.K/self.N
        X=self.X
        tau=self.tau
        D=(2*K*(1-X)+tau)    
        C0=(tau+2*K*X)/D
        C1=(tau-2*K*X)/D
        C2=(2*K*(1-X)-tau)/D
        for i in range(0,len(self.Inflow)-1,1):
            self.InitialConditions[0][0]=self.Inflow[i]
            self.InitialConditions[1][0]=self.Inflow[i+1]
            for j in range(1,self.N+1,1):
                for t in range(0,self.M,1):
                    self.InitialConditions[1][j]=C0*self.InitialConditions[0][j-1]+C1*self.InitialConditions[1][j-1]+C2*self.InitialConditions[0][j]
                    self.InitialConditions[0][j]=self.InitialConditions[1][j]
            self.Outflow[i+1]=max(self.InitialConditions[1][self.N],0)    

#2.C Tránsito Lineal con funciones de transferencia. Por defecto, se asume una distrinución gamma con parámetros n (número de reservorios) y k (tiempo de residencia). Asimismo, se considera n=2, de modo tal que tp=k (el tiempo al pico es igual al tiempo de residencia) 
class LinearChannel:
    """
    Método de tránsito hidrológico implementado sobre la base de teoría de sistemas lineales. Así, considera al tránsito de energía, materia o información como un proceso lineal desde un nodos superior hacia un nodo inferior. Específicamente, sea I=[I1,I2,...,IN] el vector de pulsos generados por el borde superior y U=[U1,U2,..,UM] una función de distribución que representa el prorateo de un pulso unitario durante el tránsito desde un nodo superior (borde) hacia un nodo inferior (salida), el sistema opera aplicando las propiedades de proporcionalidad y aditividad, de manera tal que es posible propagar cada pulso a partir de U y luego mediante la suma de estos prorateos obtener el aporte de este tránsito sobre el nodo inferior (convolución).
    """
    type='Single Linear Channel'
    def __init__(self,pars,Boundaries,Proc='Nash',dt=1):
       self.pars=np.array(pars,dtype='float')
       self.Inflow=np.array(Boundaries,dtype='float')
       self.Proc=Proc
       self.dt=dt
       if self.Proc == 'Nash':
            self.k=self.pars[0]
            self.n=self.pars[1]
            self.u=gammaDistribution(self.n,self.k,self.dt)
       if self.Proc != 'Nash':
            self.u=self.pars
       self.Outflow=np.array([[0]]*(len(self.Inflow)+len(self.u)-1))
    def computeOutFlow(self):
        I=getPulseMatrix(self.Inflow,self.u)
        self.Outflow=np.dot(I,self.u)

class LinearNet:
    """
    Método de tránsito hidrológico implementado sobre la base de teoría de sistemas lineales. Así, considera al tránsito de energía, materia o información como un proceso lineal desde N nodos superiores hacia un nodo inferior. Específicamente, sea I=[I1,I2,...,IN] un vector de pulsos generados por un borde y U=[U1,U2,..,UM] una función de distribución que representa el prorateo de un pulso unitario durante el tránsito desde un nodo superior (borde) hacia un nodo inferior (salida), aplicando las propiedades de proporcionalidad y aditividad es posible propagar cada pulso a partir de U y luego mediante su suma obtener el aporte de este tránsito sobre el nodo inferior, mediante convolución. Numéricamente el sistema se representa como una transformación matricial (matriz de pulsos*u=vector de aportes). Consecuentemente, el tránsito se realiza para cada borde y la suma total de estos tránsitos constituye la señal transitada sobre el nodo inferior.  Condiciones de borde: array 2D con hidrogramas en nodos superiores del tramo, por columna. Parámetros: función de distribución (proc='EmpDist') o tiempo de residencia (k) y número de reservorios (n), si se desea utilizar el método de hidrograma unitario de Nash (proc='Nash'), pars es un array bidimensional en donde la información necesaria para cada nodo se presenta por fila (parámetros de nodo). El parámetro dt refiere a la longitud de paso de cálculo para el método de integración, siendo dt=1 la resolución nativa de los hidrogramas de entrada provistos. Importante, las funciones de transferencia deben tener la misma cantidad de ordenadas (dimensión del vector) 
    """
    type='Linear Routing System. System of Linear Channels'
    def __init__(self,pars,Boundaries=[0],Proc='Nash',dt=1):
        self.pars=np.array(pars,dtype='float')
        self.Inflows=np.array(Boundaries,dtype='float')
        self.Proc=Proc
        self.dt=dt
    def computeOutflow(self):
        j=0
        for channel_j in range(1,len(self.Inflows[0,:])+1):
            linear=LinearChannel(pars=self.pars[j,:],Boundaries=self.Inflows[:,j],dt=self.dt)
            linear.computeOutFlow()
            if j==0:
                self.Outflow=linear.Outflow
            if j>0:
                nrows=max(len(self.Outflow),len(linear.Outflow))
                f=np.zeros((nrows))
                if len(self.Outflow) > len(linear.Outflow):
                   f[0:len(linear.Outflow)]=linear.Outflow[0:len(linear.Outflow)]
                   self.Outflow=self.Outflow+f 
                else:
                   f[0:len(self.Outflow)]=self.Outflow[0:len(self.Outflow)]
                   self.Outflow=f+linear.Outflow 
            j=j+1

if __name__ == "__main__":
    import sys

#3. Modelos PQ/QQ


    
