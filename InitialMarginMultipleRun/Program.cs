using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using Hartree.Util;
using Hetco.Util;
using RestSharp;

namespace InitialMarginMultipleRun {
    public class ParameterFile {
        protected string FileDirectory = @"\\gateway\hetco\P003\Tasks\PCSPAN\Download\";

        private readonly string zipFileName;
        private readonly string oldFileName;
        private readonly string newFileName;

        protected DateTime Date;
        protected string ConfigFileDirectory => @"\\gateway\hetco\P003\Tasks\PCSpan\Span4\Automation\";

        public string OldFileName => FileDirectory + oldFileName;
        public string ZipFileName => FileDirectory + zipFileName;
        public string NewFileName => ConfigFileDirectory + newFileName;

        public ParameterFile(DateTime dt, string zipFileName, string oldFileName, string newFileName, IList<ParameterFile> parameters) {
            Date = dt;
            this.zipFileName = zipFileName;
            this.oldFileName = oldFileName;
            this.newFileName = newFileName;
            parameters.Add(this);
        }

        public virtual void ConfigureSetting() {
            var i = 0;
            for (; i > -15; i--)
                if (File.Exists(string.Format(ZipFileName, Date.AddDays(i)))) break;
                else Console.Error.WriteLine("Daily File Not Found: {0}", string.Format(OldFileName, Date.AddDays(i)));
            ZipFileUtility.ExtractZipFile(string.Format(ZipFileName, Date.AddDays(i)), FileDirectory);
            File.Delete(NewFileName);
            File.Move(Directory.GetFiles(FileDirectory, string.Format(oldFileName, Date.AddDays(i)))[0], NewFileName);
        }
    }

    public class RawPosition : Position {
        public string PortfolioFirm;

        public RawPosition(string portfolioFirm, string exchange, string subExchange, string logical, string physical, DateTime startDate, DateTime endDate, double size, string tradeType, string optionType, double strike, string trade_type) : base(exchange, subExchange, logical, physical, startDate, endDate, size, tradeType, optionType, strike, trade_type) => PortfolioFirm = portfolioFirm;
    }

    public abstract class PortfolioRetriever {
        public string SaveFile { get; }
        public string UniqueId { get; }

        public PortfolioRetriever(string saveFile) {
            UniqueId = Guid.NewGuid().ToString();
            SaveFile = saveFile;
        }

        protected abstract List<RawPosition> RetrievePositions();

        public IList<Tuple<string, Position>> Retrieve() {
            var l = new List<Tuple<string, Position>>();
            foreach (var p in RetrievePositions())
                l.Add(Tuple.Create($"{UniqueId}_{p.PortfolioFirm}", new Position(p.Exchange, p.SubExchange, p.Logical, p.Physical, p.StartDate, p.EndDate, p.Size, p.TradeType, p.OptionType, p.Strike, p.Type)));
            return l;
        }
    }

    public class PortfolioRetrieveSql : PortfolioRetriever {
        private string SqlQuery { get; }

        public PortfolioRetrieveSql(string saveFile, string sqlQuery) : base(saveFile) => SqlQuery = sqlQuery;

        protected override List<RawPosition> RetrievePositions() {
            var positions = new List<RawPosition>();
            var dt = DatabaseClient.RunSingleTableQuery("tempestSnapshot", SqlQuery);
            for (var i = 0; i < dt.Rows.Count; i++) {
                var portfolioFirm = dt.Rows[i]["portfolio_firm"].ToString();
                var exchange = dt.Rows[i]["ecPort_ec"].ToString();
                var subExchange = dt.Rows[i]["np_exch"].ToString();
                var logical = dt.Rows[i]["ccPort_cc"].ToString();
                var physical = dt.Rows[i]["np_pfCode"].ToString();
                var startDate = Convert.ToDateTime(dt.Rows[i]["np_period"]);
                var endDate = startDate;
                var size = Convert.ToDouble(dt.Rows[i]["np_net"]);
                var optionType = dt.Rows[i]["np_optionInd"].ToString();
                var strike = dt.Rows[i]["np_strike"] == DBNull.Value ? 0 : Convert.ToDouble(dt.Rows[i]["np_strike"]);
                var tradeType = dt.Rows[i]["np_pfType"].ToString();
                var type = dt.Rows[i]["trade_type"].ToString();
                positions.Add(new RawPosition(portfolioFirm, exchange, subExchange, logical, physical, startDate, endDate, size, tradeType, optionType, strike, type));
            }
            return positions;
        }
    }

    public interface BlackBox {
        string PositionFile { get; }
        string ParameterFile { get; }
    }

    public abstract class _Exchange {
        public List<Portfolio> Portfolios { get; }
        public string Exchange { get; }
        public abstract string ResultFile { get; }

        public _Exchange(string exchange, Dictionary<string, List<Portfolio>> portfolios, IList<_Exchange> exchanges) {
            Portfolios = portfolios.ContainsKey(exchange) ? portfolios[exchange] : new List<Portfolio>();
            Exchange = exchange;
            exchanges.Add(this);
        }

        public abstract void BuildPositionFile();
        public abstract void CalculateMargin();
        public abstract void Run();
    }

    public interface _Init {
        void Init();
    }

    public class Nodal : _Exchange, _Init {
        public override string ResultFile { get; }
        private int ExitCode;
        private Dictionary<string, string> PortfoliosFile { get; }
        private HashSet<string> LotContracts;

        public Nodal(Dictionary<string, List<Portfolio>> portfolios, string resultFile, IList<string> marginFiles, IList<_Exchange> exchanges) : base("NDL", portfolios, exchanges) {
            ResultFile = resultFile;
            marginFiles.Add(resultFile);
            PortfoliosFile = new Dictionary<string, string>();
        }

        public void Init() {
            LotContracts = new HashSet<string>();
            var dt = DatabaseClient.RunSingleTableQuery("Workspace", "SELECT commodity_code FROM nodal_unit_mapping WHERE unit = 'LOTS'");
            for (var i = 0; i < dt.Rows.Count; i++)
                LotContracts.Add(dt.Rows[i]["commodity_code"].ToString());
        }

        public override void BuildPositionFile() {
            foreach (var p in Portfolios) {
                var b = new StringBuilder().AppendLine(@"physical_commodity_code,contract_term_code,contract_type,expiry,strike_price,net_lots,units");
                foreach (var c in p.Contracts)
                    foreach (var _p in c.Positions)
                        b.AppendLine(string.Format("{0},f,{1},{2},{3},{4},{5}", _p.Physical, _p.OptionType == "" ? "f" : _p.OptionType.ToLower(), _p.StartDate.ToString("yyyyMM") + "00", _p.Strike / 100, _p.Size, LotContracts.Contains(_p.Physical) ? "LOTS" : "MW"));
                PortfoliosFile.Add(p.Name, b.ToString());
            }
        }

        public override void CalculateMargin() {
            var l = new List<string>();
            using (var file = new StreamWriter(ResultFile, false)) {
                file.WriteLine("Portfolio,Exchange,Commodity,Delta,Maint Margin,Opt Value,SPAN Requirement,Scan Risk,Inter Credit,Intra Charge,Init Req,Init Margin,Is Maint");
                foreach (var k in PortfoliosFile.Keys)
                    try {
                        var m = CalculateMargin(PortfoliosFile[k]);
                        file.WriteLine($"{k},NODAL,NODAL,,,,,,,,{m},{m},0");
                    }
                    catch (Exception) {
                        ExitCode = 1;
                        l.Add(k);
                    }
            }
            Console.WriteLine($"Process exited: {ExitCode} {Exchange}");
            foreach (var _l in l)
                Console.Error.WriteLine($"Nodal Book {_l} Failed.");
        }

        private double CalculateMargin(string portfolio) {
            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true;
            var uri = new Uri("https://apps2.nodalexchange.com/idp/rest/security/login");
            var cookieJar = new CookieContainer();
            var client = new RestClient {
                BaseUrl = uri,
                CookieContainer = cookieJar,
                UserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"
            };
            var request = new RestRequest { Method = Method.POST };
            request.AddParameter("redirectUrl-inputEl", "");
            request.AddParameter("username", "c_wu");
            request.AddParameter("password", "%CH)%51at%");

            var response = client.Execute(request);

            if (response.Headers.Where(n => n.Name == "Set-Cookie").Count() > 0)
                cookieJar.SetCookies(uri, response.Headers.Where(n => n.Name == "Set-Cookie").ElementAt(0).Value.ToString());

            uri = new Uri("https://volume.nodalexchange.com/riskmanager3/rest/test-trades/upload");
            client.BaseUrl = uri;
            var sendFileRequest = new RestRequest { Method = Method.POST };

            sendFileRequest.AddHeader("Host", "volume.nodalexchange.com");
            sendFileRequest.AddHeader("Origin", "https://volume.nodalexchange.com");
            sendFileRequest.AddHeader("Referer", "https://volume.nodalexchange.com/riskmanager/");

            sendFileRequest.AddFile("filepath", new ASCIIEncoding().GetBytes(portfolio), "test.csv", "application/vnd.ms-excel");

            var response2 = client.Execute(sendFileRequest);
            var referenceId = Regex.Match(response2.Content, "referenceId\"\\:\"([^\"]+)").Groups[1];
            var hashCode = Regex.Match(response2.Content, "hashCode\"\\:(\\d+)").Groups[1];
            var ownerId = Regex.Match(response2.Content, "ownerId\"\\:(\\d+)").Groups[1];
            var dateCreated = Regex.Match(response2.Content, "dateCreated\"\\:(\\d+)").Groups[1];
            var unixTimestamp = (int)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
            var getMarginRequest = new RestRequest("https://volume.nodalexchange.com/riskmanager3/rest/var/calculate-net-var?forecast=false&client=true&_dc=" + unixTimestamp + "&testTradeUploadReferenceIds=" + referenceId + "&page=1&start=0&limit=25", Method.GET);
            var responseMargin = client.Execute(getMarginRequest);
            var totalLiquidityMargin = Regex.Match(responseMargin.Content, "totalLiquidityMargin\"\\:([^,]+)").Groups[1].ToString();
            var margin = Regex.Match(responseMargin.Content, "totalPriceRisk\"\\:([^,]+)").Groups[1].ToString();

            var deleteFile = new RestRequest("https://volume.nodalexchange.com/riskmanager3/rest/test-trades/delete-upload/" + referenceId, Method.POST);
            var s = "{\"id\":" + hashCode + ",\"dateCreated\":" + dateCreated + ",\"ownerId\":" + ownerId + ",\"fileName\":\"test.csv\",\"referenceId\":\"" + referenceId + "\",\"hashCode\":" + hashCode + ",\"include\":false}";
            deleteFile.AddJsonBody(s);
            var responseDelete = client.Execute(deleteFile);

            return Convert.ToDouble(margin) + Convert.ToDouble(totalLiquidityMargin);
        }

        public override void Run() {
            BuildPositionFile();
            CalculateMargin();
        }
    }

    public class Portfolio {
        public string Exchange;
        public string Name;
        public List<Contract> Contracts;

        public Portfolio(string exchange, string name, List<Contract> contracts) {
            Exchange = exchange;
            Name = name;
            Contracts = contracts;
        }
    }

    public class Contract {
        public string SubExchange;
        public string Code;
        public List<Position> Positions;

        public Contract(string subExchange, string code, List<Position> positions) {
            SubExchange = subExchange;
            Code = code;
            Positions = positions;
        }
    }

    public class Position {
        public string Exchange;
        public string SubExchange;
        public string Logical;
        public string Physical;
        public DateTime StartDate;
        public DateTime EndDate;
        public double Size;
        public string TradeType;
        public string OptionType;
        public double Strike;
        public string Type;

        public Position(string exchange, string subExchange, string logical, string physical, DateTime startDate, DateTime endDate, double size, string tradeType, string optionType, double strike, string trade_type) {
            Exchange = exchange;
            SubExchange = subExchange;
            Logical = logical;
            Physical = physical;
            StartDate = startDate;
            EndDate = endDate;
            Size = size;
            TradeType = tradeType;
            OptionType = optionType;
            Strike = strike;
            Type = trade_type;
        }
    }

    public class ICESPAN : _Exchange, BlackBox {
        private readonly string resultFile;
        private readonly string ExchangeCode;

        public string ParameterFile { get; }
        public string PositionFile { get; }
        public override string ResultFile => resultFile + ".csv";

        public HashSet<string> Monthly = new HashSet<string>() { "M", "TFM" };

        public ICESPAN(Dictionary<string, List<Portfolio>> portfolios, ParameterFile parameterFile, string exchangeCode, string exchange, string positionFile, string resultFile, IList<_Exchange> exchanges) : base(exchange, portfolios, exchanges) {
            PositionFile = positionFile;
            this.resultFile = resultFile;
            ExchangeCode = exchangeCode;
            ParameterFile = parameterFile.NewFileName;
        }

        public override void BuildPositionFile() {
            using (var file = new StreamWriter(PositionFile))
                foreach (var p in Portfolios)
                    foreach (var c in p.Contracts)
                        foreach (var _p in c.Positions) {
                            var date = _p.StartDate.ToString("yyyyMM") + "00";
                            var tradeType = "F";
                            if (_p.Type.ToUpper().Trim() == "DAILY") {
                                tradeType = "D";
                                date = _p.StartDate.ToString("yyyyMMdd");
                            }
                            else if (Monthly.Contains(_p.Physical) && string.IsNullOrEmpty(_p.OptionType)) tradeType = "M";
                            else if (!string.IsNullOrEmpty(_p.OptionType)) tradeType = _p.OptionType;
                            var strike = _p.Strike * 100;
                            file.WriteLine("P,{0},{6},{1},{2},{3},{4},{5},DCO,H", p.Name, _p.Physical, tradeType, date, strike, _p.Size, ExchangeCode);
                        }
        }

        public override void CalculateMargin() {
            if (File.Exists(resultFile + "Log.xml")) File.Delete(resultFile + "Log.xml");
            using (var proc = new Process()) {
                proc.StartInfo.FileName = @"\\Gateway\hetco\P003\Tasks\PCSPAN\IceSpan\marbat.exe";
                proc.StartInfo.Arguments = $@"-pf {PositionFile} -rf {ParameterFile} -of {resultFile} -lf {resultFile}Log -ol -wt 100000 -ns";
                proc.EnableRaisingEvents = true;
                proc.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                proc.Start();
                proc.WaitForExit();
                Console.WriteLine($"Process exited: {proc.ExitCode} {Exchange}");
            }
        }

        public override void Run() {
            if (Portfolios.Count == 0) return;
            BuildPositionFile();
            CalculateMargin();
        }
    }

    public class PCSPAN : _Exchange, BlackBox {
        private readonly string SuperExchange;
        private readonly string InputFile;
        private readonly string SpnFile;

        public string PositionFile { get; }
        public string ParameterFile { get; }
        public override string ResultFile { get; }

        public PCSPAN(Dictionary<string, List<Portfolio>> portfolios, ParameterFile parameterFile, string superExchange, string exchange, string inputFile, string positionFile, string spnFile, string resultFile, IList<_Exchange> exchanges, List<string> marginFiles = null) : base(exchange, portfolios, exchanges) {
            SuperExchange = superExchange;
            InputFile = inputFile;
            PositionFile = positionFile;
            SpnFile = spnFile;
            ResultFile = resultFile;
            ParameterFile = parameterFile.NewFileName;
            if (marginFiles != null) marginFiles.Add(ResultFile);
        }

        public void GenerateCSVFile() => ReportGenerator(SpnFile, ResultFile);

        public virtual void BuildSpanInput() {
            using (var file = new StreamWriter(InputFile)) {
                file.WriteLine("LOG");
                file.WriteLine($"LOAD {ParameterFile}");
                file.WriteLine($"LOAD {PositionFile}");
                file.WriteLine("CALC");
                file.WriteLine($"SAVE    {SpnFile}");
                file.WriteLine("LOGSAVE {0}", SpnFile.Replace("spn", "log"));
            }
        }

        private void ReportGenerator(string spnLocation, string saveLocation) {
            using (var file = new StreamWriter(saveLocation)) {
                file.WriteLine("\"Portfolio\",\"Exchange\",\"Commodity\",\"Delta\",\"Maint Margin\",\"Opt Value\",\"SPAN Requirement\",\"Scan Risk\",\"Inter Credit\",\"Intra Charge\",\"Init Req\",\"Init Margin\",\"Is Maint\"");
                using (var reader = XmlReader.Create(spnLocation, new XmlReaderSettings() { IgnoreWhitespace = true }))
                    while (reader.ReadToFollowing("portfolio")) {
                        var portfolio = reader.ReadSubtree();
                        portfolio.ReadToFollowing("firm");
                        var portfolioName = portfolio.ReadString();
                        if (portfolio.ReadToFollowing("acctId"))
                            while (portfolio.ReadToFollowing("ecPort")) {
                                var ecPort = portfolio.ReadSubtree();
                                while (ecPort.ReadToFollowing("ccPort")) {
                                    var ccPort = ecPort.ReadSubtree();
                                    ccPort.ReadToFollowing("cc");
                                    var cc = ccPort.ReadString();
                                    if (!ccPort.ReadToFollowing("nReq")) continue;

                                    ccPort.ReadToFollowing("isM");
                                    _ = ccPort.ReadString();
                                    ccPort.ReadToFollowing("spanReq");
                                    var spanReq = Convert.ToDouble(ccPort.ReadString());
                                    ccPort.ReadToFollowing("anov");
                                    var anov = Convert.ToDouble(ccPort.ReadString());
                                    ccPort.ReadToFollowing("sr");
                                    var sr = ccPort.ReadString();
                                    ccPort.ReadToFollowing("ia");
                                    var ia = ccPort.ReadString();
                                    ccPort.ReadToFollowing("ie");
                                    var ie = ccPort.ReadString();

                                    var remainingDelta = 0.0;
                                    while (ccPort.Read())
                                        if (ccPort.NodeType == XmlNodeType.Element && ccPort.Name == "rd")
                                            remainingDelta += Convert.ToDouble(ccPort.ReadString());
                                        else if (ccPort.NodeType == XmlNodeType.Element && (ccPort.Name == "str" || ccPort.Name == "dReq")) break;

                                    if (ccPort.Name != "dReq") ccPort.ReadToFollowing("dReq");
                                    var dReq = ccPort.ReadSubtree();
                                    dReq.ReadToFollowing("isM");
                                    var _isM = ccPort.ReadString();
                                    dReq.ReadToFollowing("spanReq");
                                    var _spanReq = Convert.ToDouble(ccPort.ReadString());

                                    ccPort.ReadToFollowing("exch");
                                    var exchangeComplexCode = ccPort.ReadString();
                                    if (string.Format("{0:R}", remainingDelta).Contains("E"))
                                        file.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3:0.#####################}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\"", portfolioName, exchangeComplexCode, cc, remainingDelta, spanReq - anov, anov, spanReq, sr, ie, ia, _spanReq, _spanReq - anov, _isM);
                                    else file.WriteLine("\"{0}\",\"{1}\",\"{2}\",\"{3:R}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\"", portfolioName, exchangeComplexCode, cc, remainingDelta, spanReq - anov, anov, spanReq, sr, ie, ia, _spanReq, _spanReq - anov, _isM);
                                }
                            }
                    }
            }
        }

        public override void BuildPositionFile() {
            using (var file = new StreamWriter(PositionFile)) {
                file.WriteLine("<?xml version=\"1.0\"?>");
                file.WriteLine("<posFile>");
                file.WriteLine("<fileFormat>4.00</fileFormat>");
                file.WriteLine($"<created>{DateTime.Today:yyyyMMdd}</created>");
                file.WriteLine("<pointInTime>");
                file.WriteLine($"<date>{DateTime.Today:yyyyMMdd}</date>");
                file.WriteLine("<isSetl>1</isSetl>");
                file.WriteLine("<setlQualifier>final</setlQualifier>");
                foreach (var p in Portfolios) {
                    file.WriteLine("<portfolio>");
                    file.WriteLine($"<firm>{p.Name}</firm>");
                    file.WriteLine($"<acctId>{p.Name}</acctId>");
                    file.WriteLine("<acctType>H</acctType>");
                    file.WriteLine("<isCust>1</isCust>");
                    file.WriteLine("<seg>N/A</seg>");
                    file.WriteLine("<isNew>1</isNew>");
                    file.WriteLine("<qib>1</qib>");
                    file.WriteLine("<custPortUseLov>1</custPortUseLov>");
                    file.WriteLine("<currency>USD</currency>");
                    file.WriteLine("<ledgerBal>0.00</ledgerBal>");
                    file.WriteLine("<ote>0.00</ote>");
                    file.WriteLine("<securities>0.00</securities>");
                    file.WriteLine("<lue>0.00</lue>");
                    file.WriteLine("<ecPort>");
                    file.WriteLine($"<ec>{SuperExchange}</ec>");
                    foreach (var c in p.Contracts) {
                        file.WriteLine("<ccPort>");
                        file.WriteLine($"<cc>{c.Code}</cc>");
                        file.WriteLine("<currency>USD</currency>");
                        file.WriteLine("<pss>0</pss>");
                        foreach (var _p in c.Positions) {
                            file.WriteLine("<np>");
                            file.WriteLine("<exch>{0}</exch>", _p.SubExchange);
                            file.WriteLine("<pfCode>{0}</pfCode>", _p.Physical);
                            file.WriteLine("<pfType>{0}</pfType>", _p.TradeType);
                            file.WriteLine("<pe>{0}</pe>", _p.StartDate.ToString("yyyyMM") + (_p.Type.ToUpper().Trim() == "DAILY" ? _p.StartDate.ToString("dd") : SuperExchange.Trim().ToUpper() == "LCH" ? "00" : ""));
                            if (_p.TradeType != "FUT") {
                                file.WriteLine("<undPe>{0}</undPe>", _p.EndDate.ToString("yyyyMM") + (_p.Type.ToUpper().Trim() == "DAILY" ? _p.EndDate.ToString("dd") : SuperExchange.Trim().ToUpper() == "LCH" ? "00" : ""));
                                file.WriteLine("<o>{0}</o>", _p.OptionType);
                                file.WriteLine("<k>{0}</k>", _p.Strike);
                            }
                            file.WriteLine($"<net>{_p.Size}</net>");
                            file.WriteLine("</np>");
                        }
                        file.WriteLine("</ccPort>");
                    }
                    file.WriteLine("</ecPort>");
                    file.WriteLine("</portfolio>");
                }
                file.WriteLine("</pointInTime>");
                file.WriteLine("</posFile>");
            }
        }

        public override void CalculateMargin() {
            using (var proc = new Process()) {
                proc.StartInfo.FileName = @"\\gateway\hetco\p003\tasks\PCSPAN\Span4\bin\spanit.exe";
                proc.StartInfo.Arguments = InputFile;
                proc.EnableRaisingEvents = true;
                proc.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                proc.Start();
                proc.WaitForExit();
                Console.WriteLine($"Process exited: {proc.ExitCode} {Exchange}");
            }
        }

        public override void Run() {
            if (Portfolios.Count == 0) return;
            BuildSpanInput();
            BuildPositionFile();
            CalculateMargin();
            GenerateCSVFile();
        }
    }

    public class Currency {
        public DateTime Date;
        public double Rate { get; }

        public double this[string key] => CURRENCY_RATE_MAPPING[key];

        public Dictionary<Tuple<string, string>, string> InstrumentCurrencyMapping;

        private Dictionary<string, double> CURRENCY_RATE_MAPPING;

        public Currency EUR;
        public Currency GBP;
        public Currency JPY;
        public Currency ZAR;
        public Currency CAD;
        public Currency USD;
        public Currency AUD;

        private readonly string SQL_QUERY =
@"SELECT CASE 
		WHEN quote_def_cd = 'JPYToUSD'
			THEN 'JPYUSD'
		WHEN quote_def_cd = 'ZARToUSD'
			THEN 'ZARUSD'
		WHEN quote_def_cd = 'AUDToUSD'
			THEN 'AUDUSD'
		ELSE quote_def_cd
		END
	,close_price
FROM instrument i
INNER JOIN instrument_quote q ON i.instr_id = q.instr_id
WHERE quote_def_cd IN (
		'EURUSD'
		,'JPYToUSD'
		,'GBPUSD'
		,'ZARToUSD'
		,'CADUSD'
		,'AUDToUSD'
		)
	AND q.quote_dt = (
		SELECT MAX(quote_dt)
		FROM instrument_quote
		WHERE instr_id = (
				SELECT instr_id
				FROM instrument
				WHERE quote_def_cd = 'EURUSD'
				)
			AND quote_dt <= '{0:MM/dd/yyyy}'
		)";

        public Currency(DateTime date) => Date = date;
        private Currency(double rate) => Rate = rate;

        public static implicit operator double(Currency c) => c.Rate;

        public void RetrieveRates() {
            InstrumentCurrencyMapping = new Dictionary<Tuple<string, string>, string>();
            var dt = DatabaseClient.RunSingleTableQuery("Workspace",
@"SELECT DISTINCT span_bfc_cd
	,sub_exchange
	,currency
FROM newedge_code_mapping c
INNER JOIN newedge_exchange_mapping m ON c.pexch = m.pexch
WHERE currency IS NOT NULL
	AND span_bfc_cd IS NOT NULL");
            for (var i = 0; i < dt.Rows.Count; i++)
                InstrumentCurrencyMapping.Add(Tuple.Create(dt.Rows[i]["span_bfc_cd"].ToString(), dt.Rows[i]["sub_exchange"].ToString()), dt.Rows[i]["currency"].ToString());

            var d = DatabaseClient.RunSingleDictionaryQuery<string, double>("PFS", string.Format(SQL_QUERY, Date));
            EUR = new Currency(d["EURUSD"]);
            GBP = new Currency(d["GBPUSD"]);
            JPY = new Currency(d["JPYUSD"]);
            ZAR = new Currency(d["ZARUSD"]);
            CAD = new Currency(d["CADUSD"]);
            AUD = new Currency(d["AUDUSD"]);
            USD = new Currency(1);
            CURRENCY_RATE_MAPPING = new Dictionary<string, double>() {
                { "USD", USD.Rate },
                { "EUR", EUR.Rate },
                { "GBP", GBP.Rate },
                { "JPY", JPY.Rate },
                { "ZAR", ZAR.Rate },
                { "CAD", CAD.Rate },
                { "AUD", AUD.Rate }
            };
        }
    }

    internal class Program {
        private static DateTime PreviousWorkday() {
            var dt = DateTime.Today.AddDays(-1);
            while (dt.DayOfWeek == DayOfWeek.Sunday || dt.DayOfWeek == DayOfWeek.Saturday)
                dt = dt.AddDays(-1);
            return dt;
        }

        private static readonly string HplpPortfolioNewedge =
@"SELECT 'portfolio_firm' = 'Hplp Newedge'
	,'portfolio_acctId' = span_clearing_house_cd
	,'portfolio_acctType' = span_account_ind
	,'ecPort_ec' = span_clearing_house_cd
	,'ccPort_cc' = span_bfc_cd
	,'np_exch' = span_exchange_cd
	,'np_pfCode' = span_product_cd
	,'np_pfType' = span_trade_type_cd
	,'np_period' = sort_dt
	,period_cd
	,'np_optionInd' = pc_ind
	,'np_strike' = strike_price
	,'np_net' = SUM(net_lots)
	,trade_type
FROM SPAN_LOAD
WHERE span_product_cd <> 'NULL'
	AND report_date = (
		SELECT MAX(report_date)
		FROM Span_Load
		)
	AND span_clearing_house_cd NOT IN (
		'JSE'
		)
	AND book_cd NOT IN (
		'3690'
		,'3712'
		,'3714'
		,'AK-CXL'
		,'AK-Epsilon'
		,'AL'
		,'AL-CXL'
		,'AL-Epsilon'
		,'AP-CXL'
		,'AP-Epsilon'
		,'CB-CXL'
		,'Consult HP'
		,'CS'
		,'CS-CXL'
		,'CXL'
		,'DMA HL'
		,'DS-CXL'
		,'EC-CXL'
		,'EC-Epsilon'
		,'Epsilon'
		,'ES AbsRtn'
		,'ESAbsRnNGL'
		,'FDR1'
		,'HCMMgmt'
		,'HEP'
		,'HL-CXL'
		,'HL-Epsilon'
		,'HPPG HOUSE'
		,'JV'
		,'JY'
		,'JY-CXL'
		,'ListedPool'
		,'LnBio BV'
		,'LnCoal AP'
		,'LnCoal JY'
		,'LnCoalHL'
		,'LnEmissCS'
		,'LnEmissFX'
		,'LnEmissHL'
		,'LnMgtFees'
		,'LnMkt TA'
		,'LnMtl JO'
		,'LnPropHse'
		,'LnSteel PG'
		,'Management'
		,'PG-CXL'
		,'PG-Epsilon'
		,'Pooling'
		,'RM-CXL'
		,'SH-CXL'
		)
	AND strategy_num NOT IN (3577)
	AND clearing_broker_cd = 'Newedge'
	AND desk_cd <> 'HPPG'
GROUP BY span_clearing_house_cd
	,span_bfc_cd
	,span_exchange_cd
	,span_product_cd
	,span_trade_type_cd
	,span_account_ind
	,sort_dt
	,period_cd
	,strike_price
	,pc_ind
	,trade_type
HAVING SUM(net_lots) <> 0
ORDER BY portfolio_firm
	,portfolio_acctId
	,portfolio_acctType
	,ecPort_ec
	,ccPort_cc
	,np_exch
	,np_pfCode
	,np_pfType
	,np_period
	,period_cd
	,np_optionInd
	,np_strike
	,trade_type";

        private static readonly string HppgPortfolioNewedge =
@"SELECT 'portfolio_firm' = 'Hppg Newedge'
	,'portfolio_acctId' = span_clearing_house_cd
	,'portfolio_acctType' = span_account_ind
	,'ecPort_ec' = span_clearing_house_cd
	,'ccPort_cc' = span_bfc_cd
	,'np_exch' = span_exchange_cd
	,'np_pfCode' = span_product_cd
	,'np_pfType' = span_trade_type_cd
	,'np_period' = sort_dt
	,period_cd
	,'np_optionInd' = pc_ind
	,'np_strike' = strike_price
	,'np_net' = SUM(net_lots)
	,trade_type
FROM SPAN_LOAD
WHERE span_product_cd <> 'NULL'
	AND report_date = (
		SELECT MAX(report_date)
		FROM Span_Load
		)
	AND span_clearing_house_cd NOT IN (
		'JSE'
		)
	AND desk_cd = 'HPPG'
	AND (clearing_broker_cd = 'Newedge' or book_cd = 'LnPWR TL')
GROUP BY span_clearing_house_cd
	,span_bfc_cd
	,span_exchange_cd
	,span_product_cd
	,span_trade_type_cd
	,span_account_ind
	,sort_dt
	,period_cd
	,strike_price
	,pc_ind
	,trade_type
HAVING SUM(net_lots) <> 0
ORDER BY portfolio_firm
	,portfolio_acctId
	,portfolio_acctType
	,ecPort_ec
	,ccPort_cc
	,np_exch
	,np_pfCode
	,np_pfType
	,np_period
	,period_cd
	,np_optionInd
	,np_strike
	,trade_type";

        private static readonly string BookPortfolioNewedge =
@"SELECT 'portfolio_firm' = book_cd
	,'portfolio_acctId' = span_clearing_house_cd + '-' + clearing_broker_cd
	,'portfolio_acctType' = span_account_ind
	,'ecPort_ec' = span_clearing_house_cd
	,'ccPort_cc' = span_bfc_cd
	,'np_exch' = span_exchange_cd
	,'np_pfCode' = span_product_cd
	,'np_pfType' = span_trade_type_cd
	,'np_period' = sort_dt
	,period_cd
	,'np_optionInd' = pc_ind
	,'np_strike' = strike_price
	,'np_net' = SUM(net_lots)
	,trade_type
FROM SPAN_LOAD
WHERE span_product_cd <> 'NULL'
	AND report_date = (
		SELECT MAX(report_date)
		FROM Span_Load
		)
	AND span_clearing_house_cd NOT IN (
		'JSE'
		)
	AND desk_cd <> 'HPPG'
GROUP BY book_cd
	,span_clearing_house_cd
	,clearing_broker_cd
	,span_bfc_cd
	,span_exchange_cd
	,span_product_cd
	,span_trade_type_cd
	,span_account_ind
	,sort_dt
	,period_cd
	,strike_price
	,pc_ind
	,trade_type
HAVING SUM(net_lots) <> 0
ORDER BY book_cd
	,span_clearing_house_cd
	,clearing_broker_cd
	,span_bfc_cd
	,span_trade_type_cd
	,span_product_cd
	,sort_dt
	,strike_price
	,pc_ind";

        private static readonly string StrategyPortfolioNewedge =
@"SELECT 'portfolio_firm' = strategy_num
	,'portfolio_acctId' = span_clearing_house_cd + '-' + clearing_broker_cd
	,'portfolio_acctType' = span_account_ind
	,'ecPort_ec' = span_clearing_house_cd
	,'ccPort_cc' = span_bfc_cd
	,'np_exch' = span_exchange_cd
	,'np_pfCode' = span_product_cd
	,'np_pfType' = span_trade_type_cd
	,'np_period' = sort_dt
	,period_cd
	,'np_optionInd' = pc_ind
	,'np_strike' = strike_price
	,'np_net' = SUM(net_lots)
	,trade_type
FROM SPAN_LOAD
WHERE span_product_cd <> 'NULL'
	AND report_date = (
		SELECT MAX(report_date)
		FROM Span_Load
		)
	AND span_clearing_house_cd NOT IN (
		'JSE'
		)
	AND desk_cd <> 'HPPG'
GROUP BY strategy_num
	,span_clearing_house_cd
	,clearing_broker_cd
	,span_bfc_cd
	,span_exchange_cd
	,span_product_cd
	,span_trade_type_cd
	,span_account_ind
	,sort_dt
	,period_cd
	,strike_price
	,pc_ind
	,trade_type
HAVING SUM(net_lots) <> 0
ORDER BY strategy_num
	,span_clearing_house_cd
	,clearing_broker_cd
	,span_bfc_cd
	,span_trade_type_cd
	,span_product_cd
	,sort_dt
	,strike_price
	,pc_ind";

        private static void BuildPortfolioResultFile(List<string> marginFiles, PortfolioRetriever portfolioIdentifiers) {
            using (var file = new StreamWriter(portfolioIdentifiers.SaveFile)) {
                file.WriteLine("Portfolio,Exchange,Commodity,Delta,Maint Margin,Opt Value,SPAN Requirement,Scan Risk,Inter Credit,Intra Charge,Init Req,Init Margin,Is Maint");
                foreach (var f in marginFiles) {
                    var lines = Regex.Split(File.ReadAllText(f), Environment.NewLine).Skip(1);
                    foreach (var line in lines)
                        if (line.Contains($"{portfolioIdentifiers.UniqueId}_")) file.WriteLine(line.Replace($"{portfolioIdentifiers.UniqueId}_", ""));
                }
            }
        }

        private static void PcSpanResultFileCurrencyConvert(List<string> marginFiles, Currency currency) {
            for (var i = 0; i < marginFiles.Count(); i++) {
                var lines = Regex.Split(File.ReadAllText(marginFiles[i]), Environment.NewLine);
                var newFileName = marginFiles[i].Replace(Path.GetFileName(marginFiles[i]), $"_{Path.GetFileName(marginFiles[i])}");
                marginFiles[i] = newFileName;
                using (var w = new StreamWriter(newFileName, false)) {
                    w.WriteLine(lines[0]);
                    foreach (var line in lines.Skip(1)) {
                        if (string.IsNullOrEmpty(line)) continue;
                        var p = line.Split(',').Select(n => n.Trim('"')).ToArray();
                        var t = Tuple.Create(p[2], p[1]);
                        if (currency.InstrumentCurrencyMapping.TryGetValue(t, out var v)) {
                            var r = currency[v];
                            w.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}",
                                p[0],
                                p[1],
                                p[2],
                                r * Convert.ToDouble(p[3]),
                                r * Convert.ToDouble(p[4]),
                                r * Convert.ToDouble(p[5]),
                                r * Convert.ToDouble(p[6]),
                                r * Convert.ToDouble(p[7]),
                                r * Convert.ToDouble(p[8]),
                                r * Convert.ToDouble(p[9]),
                                r * Convert.ToDouble(p[10]),
                                r * Convert.ToDouble(p[11]),
                                r * Convert.ToDouble(p[12]));
                        }
                        else w.WriteLine(line);
                    }
                }
            }
        }

        private static void Merge(PCSPAN pcspanFile, ICESPAN icespanFile, string mergeSPANFile, Currency currency, List<string> marginFiles) {
            marginFiles.Add(mergeSPANFile);
            var d = new Dictionary<Tuple<string, string>, Tuple<double, double>>();
            var lines = File.ReadAllLines(icespanFile.ResultFile);
            foreach (var l in lines.Skip(1)) {
                var p = l.Split(',');
                d.Add(Tuple.Create(p[0], p[1]), Tuple.Create(currency[p[2]], Convert.ToDouble(p[14]) * -1));
            }
            using (var f = new StreamWriter(mergeSPANFile)) {
                lines = File.ReadAllLines(pcspanFile.ResultFile);
                f.WriteLine(lines[0]);
                var exch = "";
                foreach (var l in lines.Skip(1)) {
                    var p = l.Split(',').Select(n => n.Trim('"')).ToArray();
                    var t = Tuple.Create(p[2], p[1]);
                    var r = 1.0;
                    if (d.TryGetValue(t, out var v)) r = v.Item1;
                    else if (currency.InstrumentCurrencyMapping.TryGetValue(Tuple.Create(p[2], p[1]), out var cr)) r = currency[cr];
                    exch = p[1];
                    f.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}",
                        p[0],
                        p[1],
                        p[2],
                        r * Convert.ToDouble(p[3]),
                        r * Convert.ToDouble(p[4]),
                        r * Convert.ToDouble(p[5]),
                        r * Convert.ToDouble(p[6]),
                        r * Convert.ToDouble(p[7]),
                        r * Convert.ToDouble(p[8]),
                        r * Convert.ToDouble(p[9]),
                        r * (v != null ? v.Item2 : Convert.ToDouble(p[10])),
                        r * (v != null ? v.Item2 - Convert.ToDouble(p[5]) : Convert.ToDouble(p[11])),
                        r * Convert.ToDouble(p[12]));
                    d.Remove(t);
                }
                foreach (var k in d.Keys)
                    f.WriteLine($"{k.Item1},{exch},{k.Item2},,,,,,,,{d[k].Item1 * d[k].Item2},{d[k].Item1 * d[k].Item2},0");
            }
        }

        private static void Build(PortfolioRetriever retriever, Dictionary<string, List<Portfolio>> dlp) {
            var ps = retriever.Retrieve();
            var dp = new Dictionary<Tuple<string, string, string>, List<Position>>();
            foreach (var p in ps) {
                var portfolioFirm = $"{retriever.UniqueId}_{p.Item1}";
                var t = Tuple.Create(p.Item2.SubExchange.ToString(), p.Item2.Logical.ToString(), portfolioFirm.ToString());
                if (dp.ContainsKey(t)) dp[t].Add(p.Item2);
                else dp.Add(t, new List<Position>() { p.Item2 });
            }
            var dc = new Dictionary<string, List<Contract>>();
            foreach (var k in dp.Keys)
                if (dc.ContainsKey($"{k.Item1} {k.Item3}")) dc[$"{k.Item1} {k.Item3}"].Add(new Contract(k.Item1, k.Item2, dp[k]));
                else dc[$"{k.Item1} {k.Item3}"] = new List<Contract>() { new Contract(k.Item1, k.Item2, dp[k]) };
            foreach (var k in dc.Keys) {
                var parts = k.Split(' ');
                if (!dlp.ContainsKey(parts[0])) dlp.Add(parts[0], new List<Portfolio>() { new Portfolio(parts[0], parts[1], dc[k]) });
                else dlp[parts[0]].Add(new Portfolio(parts[0], parts[1], dc[k]));
            }
        }

        private static void Main(string[] _) {
            Directory.CreateDirectory(@"C:\InitialMargin");

            var dlp = new Dictionary<string, List<Portfolio>>();

            var Portfolios = new List<PortfolioRetriever> {
                new PortfolioRetrieveSql(@"C:\InitialMargin\Hplp_Result_File.csv", HplpPortfolioNewedge),
                new PortfolioRetrieveSql(@"C:\InitialMargin\Hppg_Result_File.csv", HppgPortfolioNewedge),
                new PortfolioRetrieveSql(@"C:\InitialMargin\Hplp_Book_Result_File.csv", BookPortfolioNewedge),
                new PortfolioRetrieveSql(@"C:\InitialMargin\Hplp_Strategy_Result_File.csv", StrategyPortfolioNewedge)
            };

            foreach (var p in Portfolios)
                Build(p, dlp);

            var dt = PreviousWorkday();

            var parameters = new List<ParameterFile>();

            var cmeP = new ParameterFile(dt, "cme.{0:yyyyMMdd}.s.pa2.zip", "cme.{0:yyyyMMdd}.s.pa2", "cmeDaily.pa2", parameters);
            var iceP = new ParameterFile(dt, "IPE{0:yyyyMMdd}F.SP6.zip", "IPE{0:MMdd}F.SP6", "iceDaily.SP6", parameters);
            var sgxP = new ParameterFile(dt, "sgx.{0:yyyyMMdd}.z.zip", "sgx.{0:yyyyMMdd}.z.pa2", "sgxDaily.pa2", parameters);
            var ipeP = new ParameterFile(dt, "IPE{0:yyyyMMdd}F.CSV.zip", "IPE{0:MMdd}F.csv", "iceDaily.csv", parameters);
            var nybP = new ParameterFile(dt, "NYB{0:yyyyMMdd}F.CSV.zip", "NYB{0:MMdd}F.csv", "nybDaily.csv", parameters);
            var lmeP = new ParameterFile(dt, "lme.{0:yyyyMMdd}.s.dat.zip", "{0:yyyyMMdd}_??????_SPF.dat", "lmeDaily.dat", parameters);
            var _nybP = new ParameterFile(dt, "NYB{0:yyyyMMdd}F.SP6.zip", "NYB{0:MMdd}F.SP6", "nybDaily.SP6", parameters);

            foreach (var p in parameters)
                p.ConfigureSetting();

            var marginFiles = new List<string>();

            var exchanges = new List<_Exchange>();

            var ndl = new Nodal(dlp, @"C:\InitialMargin\NDL_Margin.csv", marginFiles, exchanges);
            var cmx = new PCSPAN(dlp, cmeP, "CME", "CMX", @"C:\InitialMargin\CMXDaily.txt", @"C:\InitialMargin\CMX.xml", @"C:\InitialMargin\CMX.spn", @"C:\InitialMargin\CMX_Margin.csv", exchanges, marginFiles);
            var cbt = new PCSPAN(dlp, cmeP, "CME", "CBT", @"C:\InitialMargin\CBTDaily.txt", @"C:\InitialMargin\CBT.xml", @"C:\InitialMargin\CBT.spn", @"C:\InitialMargin\CBT_Margin.csv", exchanges, marginFiles);
            var sgx = new PCSPAN(dlp, sgxP, "SGX", "SG", @"C:\InitialMargin\SGXDaily.txt", @"C:\InitialMargin\SGX.xml", @"C:\InitialMargin\SG.spn", @"C:\InitialMargin\SG_Margin.csv", exchanges, marginFiles);
            var cme = new PCSPAN(dlp, cmeP, "CME", "CME", @"C:\InitialMargin\CMEDaily.txt", @"C:\InitialMargin\CME.xml", @"C:\InitialMargin\CME.spn", @"C:\InitialMargin\CME_Margin.csv", exchanges, marginFiles);
            var nym = new PCSPAN(dlp, cmeP, "CME", "NYM", @"C:\InitialMargin\NYMDaily.txt", @"C:\InitialMargin\NYM.xml", @"C:\InitialMargin\NYM.spn", @"C:\InitialMargin\NYM_Margin.csv", exchanges, marginFiles);
            var lme = new PCSPAN(dlp, lmeP, "LCH", "LME", @"C:\InitialMargin\LMEDaily.txt", @"C:\InitialMargin\LME.xml", @"C:\InitialMargin\LME.spn", @"C:\InitialMargin\LME_Margin.csv", exchanges, marginFiles);
            var ipePcSpan = new PCSPAN(dlp, iceP, "LCH", "IPE", @"C:\InitialMargin\IPEDaily.txt", @"C:\InitialMargin\IPE.xml", @"C:\InitialMargin\IPE.spn", @"C:\InitialMargin\IPE_Margin.csv", exchanges);
            var nybPcSpan = new PCSPAN(dlp, _nybP, "LCH", "NYB", @"C:\InitialMargin\NYBDaily.txt", @"C:\InitialMargin\NYB.xml", @"C:\InitialMargin\NYB.spn", @"C:\InitialMargin\NYB_Margin.csv", exchanges);
            var ipe = new ICESPAN(dlp, ipeP, "I", "IPE", @"C:\InitialMargin\ICE.csv", @"C:\InitialMargin\IPE_Result", exchanges);
            var nyb = new ICESPAN(dlp, nybP, "N", "NYB", @"C:\InitialMargin\NYB.csv", @"C:\InitialMargin\NYB_Result", exchanges);

            foreach (var e in exchanges) {
                if (e is _Init) (e as _Init).Init();
                e.Run();
            }

            var currency = new Currency(dt);
            currency.RetrieveRates();

            PcSpanResultFileCurrencyConvert(marginFiles, currency);

            Merge(ipePcSpan, ipe, @"C:\InitialMargin\IPE_Merge.csv", currency, marginFiles);
            Merge(nybPcSpan, nyb, @"C:\InitialMargin\NYB_Merge.csv", currency, marginFiles);

            foreach (var p in Portfolios)
                BuildPortfolioResultFile(marginFiles, p);
        }
    }
}
