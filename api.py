# Copyright 2020-2021 by Anish Koyamparambath and University of Bordeaux. All Rights Reserved.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies and that
# both that copyright notice and this permission notice appear in
# supporting documentation, and that the name of Anish Koyamparambath (AK) or 
# University of Bordeaux (UBx) will not be used in advertising or publicity pertaining 
# to distribution of the software without specific, written prior permission.
# BOTH AK AND UBx DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
# BOTH AK AND UBx BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR
# ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
# IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.



from urllib.request import Request, urlopen

import json, pandas as pd , sys  

"""
The first two classes are error classes that shall be raised in order to break
an operation or close an operation. The users are free to modify this class.
""" 
class APIError(Exception): 
    def __init__(self, ):
        pass
class RUNError(Exception):
    def __init__(self, value):
        if value == 1:
            sys.exit(1)

"""
The main calculation class that shall be inherted in the main class of GeoPolRisk module.
This class mainly has methods intended for connecting to API and calculation.
The main methodological calculation is embeded within the methods defined here
"""
class comtrade:
    
    """
    The following method connects to the COMTRADE API using request from urlopen module.
    Several inputs required to connect are provided as optional arguments. The user must
    modify the values of these optional arguments before calling the calculation function. 
    
    """
    #Method 1
    def traderequest(self, frequency = "A", 
        classification = "HS",
        period = "2010",
        partner = "all",
        reporter = "276",
        HSCode = "2602",
        TradeFlow = "1"):
        _request = "https://comtrade.un.org/api/get?max=50000&type=C&freq="+frequency+"&px="+classification+"&ps="+period+"&r="+reporter+"&p="+partner+"&cc="+HSCode+"&rg="+TradeFlow+"&fmt=json"
        
        """
        Section 1.1 connects to the COMTRADE API using the requests method of urlopen library.
        Variable counter and totcounter counts the number of requests made to the COMTRADE API,
        as the number of connection per hour is limited to 100 as a public user. The connections 
        are logged in the end of script. User must call Method 7 of GeoPolRisk module in order to be logged.
        ----> Remember Method 7 requires Method 6 to be called as a prerequisite.
        """
        #1.1 Section to connect to the COMTRADE API
        self.logging.debug(_request)
        try:
            self.counter += 1
            self.totcounter += 1
            request = Request(_request)
        except Exception as e:
            self.logging.debug(e)
            raise APIError
        response = urlopen(request)
        try:
            elevations = response.read()
        except Exception as e:
            self.logging.debug(e)
            raise APIError
        data = json.loads(elevations)
        data = pd.json_normalize(data['dataset'])
        if data.shape[0] !=0:
            Worldindex = data.ptCode.to_list().index(0)
            self.data = data.drop(data.index[[Worldindex]])
            self.code = self.data.ptCode.to_list()
            self.countries = self.data.ptTitle.to_list()
            self.quantity = self.data.TradeQuantity.to_list()
            
            """
            Section 1.2 is dedicated to calculation of a part of second factor of GeoPolRisk method also
            known as Weighted Trade Average (WTA). The trade information is weighted with the WGI values
            pulled from the csv file in the library. 
            """
            #1.2 Section to calculate the numerator and trade total
            self.WGI = pd.read_excel('./lib/NOR.xlsx', sheet_name = 'INVNOR')
            self.WGI.columns = self.WGI.columns.astype(str)
            self.WGI_year = [str(i) for i in self.WGI.Year.to_list()]
            index = self.WGI_year.index(period)
            self.WGI_score = []
            for i in self.code:
                if str(i) in self.WGI.columns.to_list():
                    self.WGI_score.append(self.WGI[str(i)].tolist()[index])
            zipped_list = zip(self.quantity, self.WGI_score)
            try:
                self.wgiavg = [x * y for (x,y) in zipped_list]
            except TypeError as e:
                self.logging.debug(e)
                self.logging.debug("The Comtrade API is broken")
                raise APIError
            try:
                self.numerator = sum(self.wgiavg)
                self.tradetotal = sum(self.quantity)
            except Exception as e:
                self.logging.debug(e)
                raise APIError
        else:
            self.numerator = 0
            self.tradetotal = 0
            self.logging.warning("The dataframe is empty")
            
    """
    The first factor of the GeoPolRisk method involved calculating the herfindahl-hirschmann
    index (hhi) and total domestic production required for calculating the second factor (WTA).
    Regions, economic units or trade blocs such as the EU can also be a part of the calculation.
    However, COMTRADE by default has the aggregated data for the EU, for others such as OECD etc 
    the code has to be manipulated inorder to aggregate the trade data. The regions has to be first
    defind in the 'regions' method in GeoPolRisk module. The production information is available in 
    excel file in library.
    """
    #Method 2
    def productionQTY(self, Element, EconomicUnit):
        if EconomicUnit[0] == "EU28":
            EconomicUnit = self.EU
        try:
            if Element in ['Cerium', 'Lanthanum']:
                Element = 'Rare Earth'
            x = pd.read_excel(self.prod_path, sheet_name = Element)
            prod = pd.DataFrame(x)
            Col = prod.columns.tolist()
        except Exception as e:
            self.logging.warning("There was an error while acessing the file Metals_Raw.xlsx with an exception as ", exc_info = True)
            sys.exit(1)

        #P2. Fetching the production quantity from 'prod' dataframe.
        self.Prod_Year = prod.Year.to_list()
        temp = [0]*len(self.Prod_Year)
        for i in EconomicUnit:
            if i in Col:
                Prod_Qty = prod[i].values.tolist()
                for k in range(len(Prod_Qty)):
                    if str(Prod_Qty[k]) == 'nan':
                        Prod_Qty[k] = 0
                self.Prod_Qty = [sum(j) for j in zip(temp, Prod_Qty)]
                temp = self.Prod_Qty
            else:
                self.Prod_Qty = temp
        #logging.debug("The following will be the list of data", "This is the country "+str(i), "Next should be the list ",str(self.Prod_Qty))
       
        #P3. Calculating the HHI.
        Nom = pd.Series()
        for i in range(1,prod.shape[1]):
            temp = prod.iloc[:,i]*prod.iloc[:,i]
            Nom = Nom.add(temp, fill_value=0)
        DeNom = prod.sum(axis = 1)
        HHI = (Nom /(DeNom*DeNom)).tolist() 
        self.HHI = [round(i,3) for i in HHI]
    """
    Main calculation class that will call the required methods and calculate the GeoPolRisk of a
    raw material to a country/region for a period. 
    For all user not trying to manipulate each factor of the GeoPolRisk method is recommended to call
    this method by modifying the optional arguments. This method can be used as a direct input to calculate.
    Proceed with caution while modyfing this method.
    """
    #Method 3
    def TotalCalculation(self, frequency = "A", 
        classification = "HS",
        period = 2010,
        partner = "all",
        reporter = 36,
        HSCode = 2602,
        TradeFlow = "1" ):
        
        """
        self.run is a method from GeoPolRisk Module. It ensures all the required methods are
        precalled before proceeding.
        """
        self.run()
        """
        The setpath method is called in the beginning to define the path for database files. 
        The reporter file has the iso codes and country names which is aligned with the
        production database. 
        """
        json_file = open(self.reporter_path, 'r')
        data = pd.json_normalize(json.loads(json_file.read())['results'])
        reporter_country = data.loc[data['id'] == str(reporter), 'text'].iloc[0]
        if reporter_country == "European Union":
            reporter_country = ["EU28"]
        else:
            reporter_country = [reporter_country]
            
        """
        hs file contains the database of hs code to the original product name. 
        Only used for logging, while metals file contain available code to the 
        raw material name which is aligned to the production database.
        """
        json_file = open(self.hs, 'r')
        data = pd.read_csv(self.metals)
        hs_element = data.loc[data['hs'] == HSCode, 'id'].iloc[0]
        self.logging.debug("The period is "+str(period)+" for the Country "+str(reporter_country[0])+" for the resource "+str(hs_element))
        
        
        
        """
        First step before proceeding is to verify if the value is pre existing in the database.
        If not it will call the API. This step is necessary because of the limits of API calls.
        The counter and totcounter logs the number of API calls.
        """
        sqlstatement = "SELECT * FROM recordData WHERE Country = '"+reporter_country[0]+"' AND Resource= '"+hs_element+"' AND Year = '"+str(period)+"';"
        row = self.select(sqlstatement)
        if len(row) == 0:
            try:
                #Call Method 2
                self.productionQTY(hs_element,reporter_country)
                #call Method 1
                self.traderequest(frequency, classification, str(period), partner, str(reporter), str(HSCode),TradeFlow)
            except Exception as e:
                self.counter -= 1
                self.logging.debug(e)
                raise APIError
            
            """
            Each of the following steps calculate the GeoPolRisk value in whole.
            The Method 2 calculates the HHI (first factor) and the total domestic production.
            The Method 1 pulls trade data and weights it with the political instability indicator 
            forming the numerator of the WTA (second factor) and the total trade value (mass).
            self.WA completes the second factor calculation and it is multiplied with the
            HHI as self.GPRS. The information is then recorded in the database also
            in an extractable form that should be chosen using the Method 7 of the GeoPolRisk module.
            """
            #3.1 Calculate the GeoPolRisk Value
            try:    
                index = self.Prod_Year.index(period)
            except ValueError as e:
                self.logging.debug(e)
                self.logging.debug("Please update Production database!")
                raise APIError
            try:
                self.WA = self.numerator/(self.tradetotal+(self.Prod_Qty[index]*1000))
            except ZeroDivisionError as e:
                self.logging.debug(e)
                self.WA = 0
            self.GPRS = self.HHI[index] * self.WA
            self.recorddata(str(period), str(self.GPRS), str(self.WA), str(self.HHI), str(reporter_country[0]), str(hs_element))
            self.logging.debug("Complete Transaction")
        else:
            self.logging.debug("No transaction has been made, as data preexists")
        self.extractdata(str(period), str(reporter_country[0]), str(hs_element), Type="csv")
           