using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Mail;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace WQMS.RTNotifiers
{
    public class RTStationMonitor
    {
        public string CheckAutmatedStationsAndNotify()
        {
            //double hoursToCheckBack = Double.Parse(ConfigurationManager.AppSettings["HoursToCheckBack"]);
            double hoursToCheckBack = -24;

            //SET START DATE AND END DATE TO CURRENT DATE TIME IN UTC. 
            //Stored procedure sp_getNotifyLimitsAndValuesByDateRange will convert to appropriate time zone for each station.
            //(as stored in database)
            var startDate = DateTime.Now.ToUniversalTime().AddHours(hoursToCheckBack);
            var endDate = DateTime.Now.ToUniversalTime();

            //Populate Reporting data table with Stations, limits, and values for date range. 
            DataTable dtStationNotifyData;
            dtStationNotifyData = loadRawDataLimitsforNotifyStations(startDate, endDate);

            //get list of stations with Notify Flag set: 
            DataTable dtNotifyStations = getNotifyStations();
            dtNotifyStations.Columns.Add("ProblemFound");
            foreach (DataRow dr in dtNotifyStations.Rows)
            {
                //need to set value to MyRow column
                dr["ProblemFound"] = 0;   // default problems to no problem ;)
            }

            Boolean bAnyProblemsFound = false;

            //strMessage parts   Feb 28, 2020. Modify so that summary can include a bit of station-specific detail. Create strMessage in parts
            string strMessage = "";
            string strMessageHeader = "";
            string strMessageBody = "";

            //limit values
            double rawDataValue = 0;
            double upperLimit = 0;
            double lowerLimit = 0;
            string varUnit = "";

            strMessageHeader = "Issues found at Automated WQ Stations \r\n\r\n";
            strMessageHeader += "Total of " + dtNotifyStations.Rows.Count.ToString() + " stations are flagged for Notification: \r\n";
            strMessageHeader += "Checking back: " + hoursToCheckBack + " hours from current time\r\n";
            strMessageHeader += "----------------------\r\n";

            //Feb 28, 2020. Comment out. This part of message will be built within station loop below.
            //foreach (DataRow dr in dtNotifyStations.Rows)
            //{
            //    strMessage += dr["stationEnvdatID"].ToString() + "\r\n";
            //}


            strMessageBody += "----------------------\r\n\r\n";

            //strMessage += "============STATION LISTING==================\r\n\r\n"; 

            //For each notify station, get a list of variables that have associated limits and data and check 
            //mesurement value against the limits
            foreach (DataRow drStatz in dtNotifyStations.Rows)
            {
                //Flag for if problem found for each specific station:
                Boolean bProblemFound = false;
                string stags;
                stags = startDate.AddHours(4).ToString();
                double dbsas = double.Parse(drStatz["UTCOffset"].ToString());


                //Add to header part of message
                strMessageHeader += drStatz["stationEnvdatID"].ToString() + "\t" + drStatz["stationName"].ToString().PadRight(72) + "\t";

                //Details part of message
                //Output Station Name and Time Zone. 
                strMessageBody += "========" + drStatz["stationEnvdatID"].ToString() + "  -  " + drStatz["stationName"].ToString() + "============\r\n\r\n";
                //strMessage += "----------------------\r\n\r\n";
                strMessageBody += "Time Period for Analysis (" + drStatz["timeZoneCode"].ToString() + ")\r\n";
                strMessageBody += "----------------------------------------------\r\n";
                strMessageBody += "Start: " + startDate.AddHours(double.Parse(drStatz["UTCOffset"].ToString())).ToString("yyyy-MM-dd HH:mm") + "\r\n";
                strMessageBody += "End:  " + endDate.AddHours(double.Parse(drStatz["UTCOffset"].ToString())).ToString("yyyy-MM-dd HH:mm") + "\r\n";
                strMessageBody += "Total: " + hoursToCheckBack + " hours\r\n";
                strMessageBody += "----------------------------------------------\r\n\r\n";
                strMessageBody += "Problem Details:\r\n";



                //look for rows of data for this station
                if (dtStationNotifyData.Select("stationID = '" + drStatz["stationID"].ToString() + "'").Length == 0)
                {

                    //No rows. Add to summary.
                    strMessageHeader += "No data transferred to database for past " + hoursToCheckBack.ToString() + " hours.\r\n";

                    //No Rows. Append Error Message to details. 
                    strMessageBody += "No data transferred to database for " + drStatz["stationEnvdatID"] + " for past " + hoursToCheckBack.ToString() + " hours. Try connecting directly to logger to troubleshoot and ensure station is down\r\n";
                    strMessageBody += "If station " + drStatz["stationEnvdatID"] + " is not active or notifications are not necessary, ask administrator to set notify flag to off\r\n";
                    bProblemFound = true;
                }
                else
                {

                    //Get Distinct Vars: 
                    //look for rows of data for this station
                    DataTable dtVars = dtStationNotifyData.Select("stationID = " + drStatz["stationID"].ToString()).CopyToDataTable().DefaultView.ToTable(true, "variableID");

                    foreach (DataRow drVarz in dtVars.Rows)
                    {
                        //select rows of data for current station and variable pair: 
                        DataRow[] foundDataForStation = dtStationNotifyData.Select("stationID = " + drStatz["stationID"].ToString() + " AND variableID = " + drVarz["variableID"].ToString());

                        for (int i = 0; i < foundDataForStation.Length; i++)
                        {
                            rawDataValue = Convert.ToDouble(foundDataForStation[i]["rawDataValue"].ToString());
                            upperLimit = Convert.ToDouble(foundDataForStation[i]["upperLimit"].ToString());
                            lowerLimit = Convert.ToDouble(foundDataForStation[i]["lowerLimit"].ToString());
                            varUnit = foundDataForStation[i]["unitValue"].ToString();
                            if (rawDataValue < lowerLimit)
                            {
                                strMessageBody += foundDataForStation[i]["variableName"].ToString() + " value: " + rawDataValue.ToString() + " " + varUnit + " at station: " + foundDataForStation[i]["stationEnvdatID"].ToString() + " at " + foundDataForStation[i]["dataMeasurementDate"] + " is below lower limit of " + lowerLimit.ToString() + "\r\n";
                                bProblemFound = true;
                            }
                            if (rawDataValue > upperLimit)
                            {
                                strMessageBody += foundDataForStation[i]["variableName"].ToString() + " value: " + rawDataValue.ToString() + " " + varUnit + " at station: " + foundDataForStation[i]["stationEnvdatID"].ToString() + " at " + foundDataForStation[i]["dataMeasurementDate"] + " is greater than upper limit of " + upperLimit.ToString() + "\r\n";
                                bProblemFound = true;
                            }
                        }
                    }

                    if (bProblemFound == false)
                    {
                        strMessageHeader += "No problems found for this station\r\n";
                        strMessageBody += "No problems found for this station\r\n";
                    }
                    else
                    {
                        strMessageHeader += "Problems found for this station. See details below.\r\n";
                    }
                }

                //Calculate the amount of change over time!
                //eg if conductivity changed by 50 us Over a 12 hour period in past 



                //add spaces before moving to next station
                strMessageBody += "\r\n\r\n";

                //if a problem was found with this station, set the overall problem found flag to true and add 
                //the station to a list of station that had problems.
                if (bProblemFound)
                {
                    bAnyProblemsFound = true;
                    drStatz["ProblemFound"] = 1;
                }


            }

            strMessageBody += "=============END SUMMARY===================\r\n\r\n";

            strMessageBody += "*************************************************************************************************************************************************\r\n";
            strMessageBody += "You have received this message because you are assigned as a contact for Automated Water Quality Station(s) that is/are experiencing problems.\r\n";
            strMessageBody += "If you are receiving this in error or no longer want notifications, reply to this email with details.\r\n";
            strMessageBody += "**************************************************************************************************************************************************\r\n\r\n";


            //********IF PROBLEM FOUND - SEND EMAIL**********
            if (bAnyProblemsFound)
            {
                //compile list of contacts email addressses that have stations with problems

                DataRow[] results = dtNotifyStations.Select("ProblemFound = 1");

                DataTable dtContacts = GetContactsForStationsWithProblems(results);

                foreach (DataRow row in dtContacts.Rows)
                {

                    if (row["techEmail"].ToString() == "genevieve.tardif@canada.ca" || row["techEmail"].ToString() == "david.benoit@canada.ca")
                    {
                    }//Do nothing if these eamils are found --quick way to not notify them instead of changing the tables in database :20 Mar 2018 S. Donohue
                    else
                    {
                        strMessageBody += row["techEmail"].ToString() + ",";
                    }
                }
                //strMessage += "shawn.donohue@canada.ca"; //also add my name here to get tagged on. Shawn Donohue 20 mar 2018
                strMessageBody += "Daniel.Bastarache@ec.gc.ca"; //Shawn Donohue 7 mar 2019 removed his name above and added David Halliwell

                strMessage += strMessageHeader + strMessageBody;
                SendEmail(strMessage, dtContacts);

            }
            else
            {
                //if no problems with stations, send email to confirm?
                //string theMessage = "No problems found at stations";
                //SendTestEmail(theMessage);
                return "";
            }


            //return message for testing purposes only. for full implementation just return void and send email
            return strMessage;
            //return "";

        }

        public void SendTestEmail(string message)
        {
            //string EmailAddresses = "david.benoit@canada.ca";
            //string EmailAddresses = "shawn.donohue@canada.ca";//changed 16 Nov 2017 as directed by D. Benoit email to rx alerts
            string EmailAddresses = "Daniel.Bastarache@ec.gc.ca";//chnaged 7 Mar 2019 to add David Halliwell to recieve email alerts
            //string fromAddress = "Charles.LeBlanc@ec.gc.ca"; //changed Dec 9, 2021

            MailMessage mail = new MailMessage();

            mail.To.Add(EmailAddresses);
            mail.From = new MailAddress("pccsm-cssp@ec.gc.ca");
            mail.IsBodyHtml = true;
            SmtpClient myClient = new System.Net.Mail.SmtpClient();
            myClient.Host = "atlantic-exgate.Atlantic.int.ec.gc.ca";

            //string subject = "FWQMS Automated Station Alert System";
            string subject = "FWQMS Automated Station Alert System";

            string msg = "<pre>" + message + "</ pre>";

            mail.Subject = subject;
            mail.Body = msg;
            myClient.Send(mail);
        }

        public void SendEmail(string message, DataTable dtContacts)
        {
            //string FromEmailAddresses = "charles.leblanc@ec.gc.ca";
            //string frEmailAddress = "david.benoit@canada.ca";
            //string frEmailAddress = "shawn.donohue@canada.ca";
            //string frEmailAddress = "Daniel.Bastarache@ec.gc.ca";//added 7 Mar 2019
            //string frEmailAddress = "Charles.LeBlanc@ec.gc.ca"; //added Dec 9 2021

            MailMessage mail = new MailMessage();

            //first hardcode Dave Benoits Email address in the case that the list of conatcts is empty. 
            // mail.To.Add("david.benoit@canada.ca");
            //mail.To.Add("shawn.donohue@canada.ca");//removed 7 mar 2019
            mail.To.Add("Daniel.Bastarache@ec.gc.ca");//added 7 Mar 2019

            string emailAddress;
            foreach (DataRow row in dtContacts.Rows)
            {
                emailAddress = row["techEmail"].ToString();
                //if (emailAddress == "david.benoit@canada.ca" || emailAddress == "genevieve.tardif@canada.ca" || emailAddress == "christine.garron@canada.ca")
                //{
                if (emailAddress == "genevieve.tardif@canada.ca" || emailAddress == "david.benoit@canada.ca")
                { }//Do nothing if these eamils are found --quick way to not notify them instead of changing the tables in database :20 Mar 2018 S. Donohue
                else
                {
                    mail.To.Add(emailAddress);//keep everyone else in the original table notified.
                }
            }

            //mail.To.Add("Charles.LeBlanc@ec.gc.ca");
            //mail.CC.Add("christine.garron@canada.ca");

            mail.From = new MailAddress("pccsm-cssp@ec.gc.ca");
            mail.IsBodyHtml = true;

            SmtpClient myClient = new System.Net.Mail.SmtpClient();
            //myClient.Host = "atlantic-exgate.Atlantic.int.ec.gc.ca";


            myClient.Host = "mail.ec.gc.ca";
            myClient.Port = 587;
            //myClient.Credentials = new System.Net.NetworkCredential("yourusername", "yourpassword");
            myClient.Credentials = new System.Net.NetworkCredential("pccsm-cssp@ec.gc.ca", "Gt=UJZ3g]8_P86Q]::p0F(%=$_OL_Y");
            myClient.EnableSsl = true;

            //string subject = "FWQMS Automated Station Alert System";
            string subject = "FWQMS Automated Station Alert System";

            string msg = "<pre>" + message + "</ pre>";

            mail.Subject = subject;
            mail.Body = msg;
            myClient.Send(mail);

        }

        protected DataTable loadRawDataLimitsforNotifyStations(DateTime dtStartTime, DateTime dtEndTime)
        {
            //string conStr = System.Configuration.ConfigurationManager.ConnectionStrings["RTPROD"].ToString();
            //string conStr = "Data Source=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Initial Catalog=ecPacificRealTimeWaterQuality;Integrated Security=True";

            //November 14, 2019. SSC has migrated the database. New server, new database name
            //string conStr = "Server=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Database=ecPacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";
            string conStr = "Server=NATSQLAPPS2\\INS2;Database=PacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";

            using (SqlConnection conn = new SqlConnection(conStr))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("sp_getNotifyLimitsAndValuesByDateRange", conn);

                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add("@startDate", SqlDbType.DateTime).Value = dtStartTime;
                cmd.Parameters.Add("@endDate", SqlDbType.DateTime).Value = dtEndTime;

                SqlDataReader dr = cmd.ExecuteReader();
                //int count;
                //count = 0;

                // load to datatable to get field count: 

                DataTable dt = new DataTable();
                dt.Load(dr);

                conn.Close();

                return dt;
            }
        }



        protected DataTable getNotifyStations()
        {
            //November 14, 2019. SSC has migrated servers. New server name, new database.
            //string conStr = "Data Source=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Initial Catalog=ecPacificRealTimeWaterQuality;Integrated Security=True";
            //string conStr = System.Configuration.ConfigurationManager.ConnectionStrings["RTPROD"].ToString();
            //string conStr = "Server=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Database=ecPacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";
            string conStr = "Server=NATSQLAPPS2\\INS2;Database=PacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";

            using (SqlConnection conn = new SqlConnection(conStr))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("sp_getStationIdsToNotify", conn);

                cmd.CommandType = CommandType.StoredProcedure;
                SqlDataReader dr = cmd.ExecuteReader();

                // load to datatable to get field count: 
                DataTable dt = new DataTable();
                dt.Load(dr);

                conn.Close();

                return dt;
            }
        }

        protected DataTable GetContactsForStationsWithProblems(DataRow[] drStations)
        {
            //set datarow to a string of comma seperated station numbers.
            string stationNumbers = "";
            for (int i = 0; i < drStations.Length; i++)
            {
                stationNumbers += drStations[i]["stationEnvdatID"] + ",";
            }
            //remove trailing comma
            stationNumbers = stationNumbers.Substring(0, stationNumbers.Length - 1);


            //SET UP CONNECTION
            //November 14, 2019. SSC has migrated servers. New server name, new database.
            //string conStr = System.Configuration.ConfigurationManager.ConnectionStrings["RTPROD"].ToString();
            //string conStr = "Data Source=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Initial Catalog=ecPacificRealTimeWaterQuality;Integrated Security=True";
            //string conStr = "Server=NATSQLAPPS2NCR.ncr.int.ec.gc.ca\\ins2;Database=ecPacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";
            string conStr = "Server=NATSQLAPPS2\\INS2;Database=PacificRealTimeWaterQuality;User Id=lndb;Password=bdn1RTWQ;";

            using (SqlConnection conn = new SqlConnection(conStr))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("sp_getTechInfoByStationNumber", conn);

                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@stnEnvdatID", stationNumbers);

                SqlDataReader dr = cmd.ExecuteReader();

                // load to datatable to get field count: 
                DataTable dt = new DataTable();
                dt.Load(dr);

                conn.Close();

                return dt;
            }


        }

    }
}
