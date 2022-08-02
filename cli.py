import src.pydrodelta.analysis as analysis
import json
import logging
import sys

if __name__ == "__main__":
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('config_file',help="location of config file (.json)", type=str)
    argparser.add_argument("-c","--csv", help = "Save result of analysis as .csv file",type=str)
    argparser.add_argument("-j","--json", help = "Save result of analysis to .json file",type=str)
    argparser.add_argument("-p","--pivot", help = "Pivot output table",action="store_true")
    argparser.add_argument("-u","--upload", help = "Upload output to database API",action="store_true")
    argparser.add_argument("-P","--include_prono", help = "Concatenate series_prono to output series",action="store_true")
    argparser.add_argument("-v","--verbose", help = "log to stdout",action="store_true")
    args = argparser.parse_args()
    if args.verbose:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)
    t_config = json.load(open(args.config_file))
    topology = analysis.Topology(t_config)
    topology.batchProcessInput(include_prono=args.include_prono)
    if args.csv is not None:
        topology.saveData(args.csv,pivot=args.pivot)
    if args.json is not None:
        topology.saveData(args.json,format="json",pivot=args.pivot)
    if args.upload:
        uploaded = topology.uploadData()
    """
    TODO: output concatenateProno
    """