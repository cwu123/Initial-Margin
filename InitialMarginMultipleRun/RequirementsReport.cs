using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;

namespace InitialMarginMultipleRun {
    internal class RequirementsReport {
        public static void ReportGenerator(string spnLocation, string saveLocation) {
            using (var file = new StreamWriter(saveLocation)) {
                file.WriteLine("\"Portfolio\",\"Exchange\",\"Commodity\",\"Delta\",\"Maint Margin\",\"Opt Value\",\"SPAN Requirement\",\"Scan Risk\",\"Inter Credit\",\"Intra Charge\",\"Init Req\",\"Init Margin\",\"Is Maint\"");
                using (var reader = XmlReader.Create(spnLocation, new XmlReaderSettings() { IgnoreWhitespace = true }))
                    while (reader.ReadToFollowing("portfolio")) {
                        var portfolio = reader.ReadSubtree();
                        portfolio.ReadToFollowing("firm");
                        var portfolioName = portfolio.ReadString();
                        if (portfolio.ReadToFollowing("acctId") & Regex.IsMatch(portfolio.ReadString(), "Test" + "$"))
                            while (portfolio.ReadToFollowing("ecPort")) {
                                var ecPort = portfolio.ReadSubtree();
                                while (ecPort.ReadToFollowing("ccPort")) {
                                    var ccPort = ecPort.ReadSubtree();
                                    ccPort.ReadToFollowing("cc");
                                    var cc = ccPort.ReadString();
                                    if (!ccPort.ReadToFollowing("nReq")) continue;

                                    ccPort.ReadToFollowing("isM");
                                    var isM = ccPort.ReadString();
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
    }
}
