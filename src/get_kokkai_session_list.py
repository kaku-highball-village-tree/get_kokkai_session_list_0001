import argparse
import csv
import datetime
import requests


# コマンドライン引数を解析して開始日と終了日を取得する
def parse_arguments() -> tuple[datetime.date, datetime.date]:
    objParser = argparse.ArgumentParser(description="国会会議録APIから本会議の回次一覧を取得する")
    objParser.add_argument("--start-date", dest="pszStartDate", type=str)
    objParser.add_argument("--end-date", dest="pszEndDate", type=str)
    objArgs = objParser.parse_args()

    objToday = datetime.date.today()
    objDefaultStartDate = objToday - datetime.timedelta(days=30)
    pszStartDate = objArgs.pszStartDate or objDefaultStartDate.isoformat()
    pszEndDate = objArgs.pszEndDate or objToday.isoformat()

    objStartDate = datetime.date.fromisoformat(pszStartDate)
    objEndDate = datetime.date.fromisoformat(pszEndDate)

    return objStartDate, objEndDate


# 国会会議録APIから本会議の会議一覧を取得して全件を返す
def fetch_meeting_records(pszStartDate: str, pszEndDate: str) -> list[dict]:
    pszApiUrl = "https://kokkai.ndl.go.jp/api/meeting_list"
    iMaximumRecords = 100
    iRecordPosition = 1
    objListMeetingRecord = []

    while True:
        objParams = {
            "nameOfMeeting": "本会議",
            "startDate": pszStartDate,
            "endDate": pszEndDate,
            "maximumRecords": iMaximumRecords,
            "recordPosition": iRecordPosition,
            "recordPacking": "json",
        }
        objResponse = requests.get(pszApiUrl, params=objParams, timeout=30)
        if objResponse.status_code != 200:
            raise RuntimeError(f"HTTPステータスが不正です: {objResponse.status_code}")
        objJson = objResponse.json()

        if "meetingRecord" not in objJson:
            raise ValueError("JSON構造が想定外です: meetingRecordが存在しません")
        objListPageRecords = objJson["meetingRecord"]
        if not isinstance(objListPageRecords, list):
            raise ValueError("JSON構造が想定外です: meetingRecordがリストではありません")

        if not objListPageRecords:
            break

        objListMeetingRecord.extend(objListPageRecords)
        iRecordPosition += iMaximumRecords

    return objListMeetingRecord


# 会議一覧から回次ごとに開始日と終了日を集約する
def aggregate_sessions(objListMeetingRecord: list[dict]) -> list[dict]:
    objSessionDateMap = {}

    for objMeetingRecord in objListMeetingRecord:
        if "session" not in objMeetingRecord or "date" not in objMeetingRecord:
            raise ValueError("JSON構造が想定外です: sessionまたはdateが存在しません")

        iSession = int(objMeetingRecord["session"])
        pszMeetingDate = objMeetingRecord["date"]
        objMeetingDate = datetime.date.fromisoformat(pszMeetingDate)

        if iSession not in objSessionDateMap:
            objSessionDateMap[iSession] = {
                "objStartDate": objMeetingDate,
                "objEndDate": objMeetingDate,
            }
        else:
            if objMeetingDate < objSessionDateMap[iSession]["objStartDate"]:
                objSessionDateMap[iSession]["objStartDate"] = objMeetingDate
            if objMeetingDate > objSessionDateMap[iSession]["objEndDate"]:
                objSessionDateMap[iSession]["objEndDate"] = objMeetingDate

    objListSession = []
    for iSession, objDateRange in objSessionDateMap.items():
        objListSession.append(
            {
                "iSession": iSession,
                "pszStartDate": objDateRange["objStartDate"].isoformat(),
                "pszEndDate": objDateRange["objEndDate"].isoformat(),
            }
        )

    objListSession = sorted(objListSession, key=lambda objItem: objItem["iSession"], reverse=True)
    return objListSession


# 集約結果をTSVファイルとして出力する
def write_tsv(objListSession: list[dict], pszOutputPath: str) -> None:
    with open(pszOutputPath, "w", encoding="utf-8", newline="") as objFile:
        objWriter = csv.writer(objFile, delimiter="\t")
        objWriter.writerow(["session", "start_date", "end_date"])
        for objSession in objListSession:
            objWriter.writerow(
                [
                    objSession["iSession"],
                    objSession["pszStartDate"],
                    objSession["pszEndDate"],
                ]
            )


# メイン処理としてAPI取得と集約とTSV出力を実行する
def main() -> None:
    objStartDate, objEndDate = parse_arguments()
    pszStartDate = objStartDate.isoformat()
    pszEndDate = objEndDate.isoformat()
    objListMeetingRecord = fetch_meeting_records(pszStartDate, pszEndDate)
    objListSession = aggregate_sessions(objListMeetingRecord)
    write_tsv(objListSession, "kokkai_session_list.tsv")


if __name__ == "__main__":
    main()
