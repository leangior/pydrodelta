import dateutil.parser
import pytz
localtz = pytz.timezone('America/Argentina/Buenos_Aires')
import pandas
from datetime import timedelta, datetime
import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import logging

def interval2timedelta(interval):
    days = 0
    seconds = 0
    microseconds = 0
    milliseconds = 0
    minutes = 0
    hours = 0
    weeks = 0
    for k in interval:
        if k == "milliseconds" or k == "millisecond":
            milliseconds = interval[k]
        elif k == "seconds" or k == "second":
            seconds = interval[k]
        elif k == "minutes" or k == "minute":
            minutes = interval[k]
        elif k == "hours" or k == "hour":
            hours = interval[k]
        elif k == "days" or k == "day":
            days = interval[k]
        elif k == "weeks" or k == "week":
            weeks = interval[k] * 86400 * 7
    return timedelta(days=days, seconds=seconds, microseconds=microseconds, milliseconds=milliseconds, minutes=minutes, hours=hours, weeks=weeks)

def interval2epoch(interval):
    seconds = 0
    for k in interval:
        if k == "milliseconds" or k == "millisecond":
            seconds = seconds + interval[k] * 0.001
        elif k == "seconds" or k == "second":
            seconds = seconds + interval[k]
        elif k == "minutes" or k == "minute":
            seconds = seconds + interval[k] * 60
        elif k == "hours" or k == "hour":
            seconds = seconds + interval[k] * 3600
        elif k == "days" or k == "day":
            seconds = seconds + interval[k] * 86400
        elif k == "weeks" or k == "week":
            seconds = seconds + interval[k] * 86400 * 7
        elif k == "months" or k == "month" or k == "mon":
            seconds = seconds + interval[k] * 86400 * 31
        elif k == "years" or k == "year":
            seconds = seconds + interval[k] * 86400 * 365
    return seconds

def tryParseAndLocalizeDate(date_string,timezone='America/Argentina/Buenos_Aires'):
    date = dateutil.parser.isoparse(date_string) if isinstance(date_string,str) else date_string
    if isinstance(date,dict):
        date = datetime.now() + interval2timedelta(date)
    if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
        try:
            date = pytz.timezone(timezone).localize(date)
        except pytz.exceptions.NonExistentTimeError:
            logging.warning("NonexistentTimeError: %s" % str(date))
            return None
    else:
        date = date.astimezone(pytz.timezone(timezone))
    return date

def roundDownDate(date : datetime,timeInterval : timedelta,timeOffset : timedelta=None) -> datetime:
    if timeInterval.microseconds == 0:
        date = date.replace(microsecond=0)
    if timeInterval.seconds % 60 == 0:
        date = date.replace(second=0)
    if timeInterval.seconds % 3600 == 0:
        date = date.replace(minute=0)
    if timeInterval.seconds == 0 and timeInterval.days >= 1:
        date = date.replace(hour=0)
        if timeOffset is not None:
            date = date + timeOffset
    return date

def roundDate(date : datetime,timeInterval : timedelta,timeOffset : timedelta=None, to="up") -> datetime:
    date_0 = tryParseAndLocalizeDate(datetime.combine(date.date(),datetime.min.time()))
    if timeOffset is not None:
        date_0 = date_0 + timeOffset 
    while date_0 < date:
        date_0 = date_0 + timeInterval
    if to == "up":
        return date_0
    else:
        return date_0 - timeInterval

def createDatetimeSequence(datetime_index : pandas.DatetimeIndex=None, timeInterval=timedelta(days=1), timestart=None, timeend=None, timeOffset=None):
    #Fechas desde timestart a timeend con un paso de timeInterval
    #data: dataframe con index tipo datetime64[ns, America/Argentina/Buenos_Aires]
    #timeOffset sólo para timeInterval n days
    if datetime_index is None and (timestart is None or timeend is None):
        raise Exception("Missing datetime_index or timestart+timeend")
    timestart = timestart if timestart is not None else datetime_index.min()
    timestart = roundDate(timestart,timeInterval,timeOffset,"up")
    timeend = timeend if timeend  is not None else datetime_index.max()
    timeend = roundDate(timeend,timeInterval,timeOffset,"down")
    return pandas.date_range(start=timestart, end=timeend, freq=pandas.DateOffset(days=timeInterval.days, hours=timeInterval.seconds // 3600, minutes = (timeInterval.seconds // 60) % 60))

def serieRegular(data : pandas.DataFrame, timeInterval : timedelta, timestart=None, timeend=None, timeOffset=None, column="valor", interpolate=True, interpolation_limit=1):
    # genera serie regular y rellena nulos interpolando
    df_regular = pandas.DataFrame(index = createDatetimeSequence(data.index, timeInterval, timestart, timeend, timeOffset))
    df_regular.index.rename('timestart', inplace=True)	 
    df_join = df_regular.join(data, how = 'outer')
    if interpolate:
        # Interpola
        df_join[column] = df_join[column].interpolate(method='time',limit=interpolation_limit,limit_direction='both')
    df_regular = df_regular.join(df_join, how = 'left')
    return df_regular

def serieFillNulls(data : pandas.DataFrame, other_data : pandas.DataFrame, column : str="valor", other_column : str="valor", fill_value : float=None, shift_by : int=0, bias : float=0, extend=False):
    """
    rellena nulos de data con valores de other_data donde coincide el index. Opcionalmente aplica traslado rígido en x (shift_by: n registros) y en y (bias: float)

    si extend=True el índice del dataframe resultante será la unión de los índices de data y other_data (caso contrario será igual al índice de data)
    """
    mapper = {}
    mapper[other_column] = "valor_fillnulls"
    how = "outer" if extend else "left"
    data = data.join(other_data[[other_column,]].rename(mapper,axis=1), how = how)
    data[column] = data[column].fillna(data["valor_fillnulls"].shift(shift_by, axis = 0) + bias)
    del data["valor_fillnulls"]
    if fill_value is not None:
        data[column] = data[column].fillna(fill_value)
    return data

def serieMovingAverage(obs_df : pandas.DataFrame,offset : timedelta, column : str="valor"):
    return obs_df[column].rolling(offset, min_periods=1).mean()

def applyTimeOffsetToIndex(obs_df,x_offset):
    original_df = obs_df[["valor",]]
    del original_df["valor"]
    obs_df.index = [x + x_offset for x in obs_df.index]
    obs_df = original_df.join(obs_df,how='outer')
    obs_df.interpolate(method='time',limit=1,inplace=True)
    obs_df = original_df.join(obs_df,how='left')
    return obs_df


def removeOutliers(data : pandas.DataFrame,limite_outliers,column="valor"):
    '''
    remove outliers inline and return outliers data frame
    '''
    # print('Detecta Outliers:')
    limit_inf = limite_outliers[0]
    limit_sup = limite_outliers[1]
    # print("Limite superior",round(limit_sup,2))
    # print("Limite inferior",round(limit_inf,2)) 
    # Finding the Outliers
    outliers_iqr = data[( data[column] < limit_inf) | (data[column] > limit_sup)]
    logging.debug('Cantidad de outliers: %i' % len(outliers_iqr))
    data[column] = np.where(data[column]>limit_sup,np.nan,
                   np.where(data[column]<limit_inf,np.nan,
                   data[column]))
    return outliers_iqr

def detectJumps(data : pandas.DataFrame,lim_jump,column="valor"):
    '''
    returns jump rows as data frame
    '''
    # print('Detecta Saltos:')	
    VecDif = abs(np.diff(data[column].values))
    VecDif = np.append([0,],VecDif)
    coldiff = 'Diff_Valor'
    data[coldiff] = VecDif
    # print('Limite Salto (m): ',lim_jump)
    df_saltos = data[data[coldiff] > lim_jump].sort_values(by=coldiff)
    logging.debug('Cantidad de Saltos: %i' % len(df_saltos))
    del data[coldiff]
    return df_saltos

def adjustSeries(sim_df,truth_df,method="lfit",plot=True,return_adjusted_series=True)  -> pandas.Series:
    if method == "lfit":
        data = truth_df.join(sim_df,how="left",rsuffix="_sim")
        lr, quant_Err =  ModelRL(data,"valor",["valor_sim"])
        # Prediccion
        df_base_aux = data[["valor_sim"]].copy().dropna()
        predict = lr.predict(df_base_aux[["valor_sim"]].values)
        if plot:
            df_base_aux["adj"] = predict
            data = data.join(df_base_aux[["adj",]],how="left")
            plt.figure(figsize=(16,8))
            plt.plot(data)
            plt.legend(data.columns)
        if return_adjusted_series:
            return data["adj"]
        else:
            return lr
        #df_base_aux['valor_predic'] = prediccion
        # df_base = df_base.join(df_base_aux['Y_predic'], how = 'outer')
    else:
        raise Exception("unknown method " + method)

def linearCombination(sim_df : pandas.DataFrame,params : dict,plot=True) -> pandas.Series:
    '''
        sim_df: DataFrame con las covariables
        params: { intercept: float, coefficients: [float,...]
        plot: Boolean
    '''

    sim_df["predict"] = params["intercept"]
    for i in range(len(params["coefficients"])):
        sim_df["predict"] += sim_df.iloc[:,i] * params["coefficients"][i]
    if plot:
        plt.figure(figsize=(16,8))
        plt.plot(sim_df)
        plt.legend(sim_df.columns)
    return sim_df["predict"]

def ModelRL(data : pandas.DataFrame, varObj : str, covariables : list):
    train = data.copy()

    ## Modelo
    train = train.dropna()

    var_obj = varObj
    covariav = covariables

    lr = linear_model.LinearRegression()
    X_train = train[covariav]
    Y_train = train[var_obj]
    lr.fit(X_train,Y_train)

    # Create the test features dataset (X_test) which will be used to make the predictions.
    X_test = train[covariav].values
    # The labels of the model
    Y_test = train[var_obj].values
    Y_predictions = lr.predict(X_test)
    train['Y_predictions'] = Y_predictions
    #train['Y_predictions'] = train['h_sim'] - train['Y_predictions'] 
    # The coefficients
    # The mean squared error
    mse = mean_squared_error(Y_test, Y_predictions)
    # The coefficient of determination: 1 is perfect prediction
    coefDet = r2_score(Y_test, Y_predictions)
    logging.debug('Coefficients B0: %.5f, coefficients: %.5f, Mean squared error: %.5f, r2_score: %.5f' % (lr.intercept_, lr.coef_, mse, coefDet))
    train['Error_pred'] =  train['Y_predictions']  - train[var_obj]
    quant_Err = train['Error_pred'].quantile([.001,.05,.95,.999])
    
    return lr,quant_Err
    
