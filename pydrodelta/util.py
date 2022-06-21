import dateutil.parser
import pytz
import pandas
from datetime import timedelta

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
    if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
        try:
            date = pytz.timezone(timezone).localize(date)
        except pytz.exceptions.NonExistentTimeError:
            print("NonexistentTimeError: %s" % str(date))
            return None
    else:
        date = date.astimezone(pytz.timezone(timezone))
    return date

def roundDownDate(date,timeInterval,timeOffset=None):
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

def createDatetimeSequence(datetime_index : pandas.DatetimeIndex=None, timeInterval=timedelta(days=1), timestart=None, timeend=None, timeOffset=None):
    #Fechas desde timestart a timeend con un paso de timeInterval
    #data: dataframe con index tipo datetime64[ns, America/Argentina/Buenos_Aires]
    #timeOffset sólo para timeInterval n days
    if datetime_index is None and (timestart is None or timeend is None):
        raise Exception("Missing datetime_index or timestart+timeend")
    timestart = timestart if timestart is not None else datetime_index.min()
    timestart = roundDownDate(timestart,timeInterval,timeOffset)
    timeend = timeend if timeend  is not None else datetime_index.max()
    timeend = roundDownDate(timeend,timeInterval,timeOffset)
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

def serieFillNulls(data : pandas.DataFrame, other_data : pandas.DataFrame, column : str="valor", other_column : str="valor", fill_value : float=None, shift_by : int=0, bias : float=0):
    # rellena nulos de data con valores de other_data donde coincide el index. Opcionalmente aplica traslado rígido en x (shift_by: n registros) y en y (bias: float)
    mapper = {}
    mapper[other_column] = "valor_fillnulls"
    data = data.join(other_data[[other_column,]].rename(mapper,axis=1), how = 'left')
    data[column] = data[column].fillna(data["valor_fillnulls"].shift(shift_by, axis = 0) - bias)
    del data["valor_fillnulls"]
    if fill_value is not None:
        data[column] = data[column].fillna(fill_value)
    return data

