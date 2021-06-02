from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def buildQuery(result: ResultSet, rows_effected: int) -> Query:
    if rows_effected != 1:
        raise Exception()
    return Query(result[0]['queryID'], result[0]['purpose'], result[0]['size'])

def buildDisk(result: ResultSet, rows_effected: int) -> Disk:
    if rows_effected != 1:
        raise Exception()
    return Disk(result[0]['diskID'], result[0]['company'], result[0]['speed'], result[0]['free_space'], result[0]['cost'])

def buildRAM(result: ResultSet, rows_effected: int) -> RAM:
    if rows_effected != 1:
        raise Exception()
    return RAM(result[0]['ramID'], result[0]['company'], result[0]['size'])

def buildAvailableDiskList(result: ResultSet, rows_effected: int) -> List[int]:
    ret = []
    for i in range(0, rows_effected):
        ret.append(result[i]['diskID'])
    return ret

def buildCloseQueriesList(result: ResultSet, rows_effected: int) -> List[int]:
    ret = []
    for i in range(0, rows_effected):
        ret.append(result[i]['queryID'])
    return ret

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "CREATE TABLE Queries ("
            "queryID INTEGER, "
            "purpose TEXT NOT NULL, "
            "size INTEGER NOT NULL, "
            "PRIMARY KEY (queryID), "
            "CHECK (queryID > 0), "
            "CHECK (size >= 0)"
            "); "

            "CREATE TABLE Disks ("
            "diskID INTEGER, "
            "company TEXT NOT NULL, "
            "speed INTEGER NOT NULL, "
            "free_space INTEGER NOT NULL, "
            "cost INTEGER NOT NULL, "
            "PRIMARY KEY (diskID), "
            "CHECK (diskID > 0), "
            "CHECK (speed > 0), "
            "CHECK (cost > 0), "
            "CHECK (free_space >= 0)"
            "); "

            "CREATE TABLE RAMs ("
            "ramID INTEGER, "
            "size INTEGER NOT NULL, "
            "company TEXT NOT NULL, "
            "PRIMARY KEY (ramID), "
            "CHECK (ramID > 0), "
            "CHECK (size > 0)"
            "); "

            "CREATE TABLE QueriesDisks ("
            "queryID INTEGER, "
            "diskID INTEGER, "
            "FOREIGN KEY (queryID) "
            "REFERENCES Queries(queryID) "
            "ON DELETE CASCADE, "
            "FOREIGN KEY (diskID) "
            "REFERENCES Disks(diskID) "
            "ON DELETE CASCADE, "
            "PRIMARY KEY (queryID, diskID)"
            "); "

            "CREATE TABLE RAMsDisks ("
            "ramID INTEGER, "
            "diskID INTEGER, "
            "FOREIGN KEY (ramID) "
            "REFERENCES RAMs(ramID) "
            "ON DELETE CASCADE, "
            "FOREIGN KEY (diskID) "
            "REFERENCES Disks(diskID) "
            "ON DELETE CASCADE, "
            "PRIMARY KEY (ramID, diskID)"
            ");"
            
            "CREATE VIEW QueriesDisksWithSizes AS "
            "SELECT q.queryID, d.diskID, q.size, d.free_space "
            "FROM Queries q " 
            "INNER JOIN QueriesDisks qd "
            "ON q.queryID = qd.queryID "
            "INNER JOIN Disks d "
            "ON qd.diskID = d.diskID; "
            
            "CREATE VIEW PossibleQueriesDisks AS "
            "SELECT q.queryID, d.diskID, d.speed "
            "FROM Queries q, Disks d "
            "WHERE q.size <= d.free_space; "
            
            "CREATE VIEW DisksRAMSum as "
            "SELECT diskID, SUM(R.size) AS ramSum "
            "FROM RAMs R, RAMsDisks RD "
            "WHERE R.ramID = RD.ramID "
            "GROUP BY rd.diskID;"
        )
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DELETE FROM Queries; "
                              "DELETE FROM Disks; "
                              "DELETE FROM RAMs;")
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DROP TABLE IF EXISTS Queries CASCADE; "
                              "DROP TABLE IF EXISTS Disks CASCADE; "
                              "DROP TABLE IF EXISTS RAMs CASCADE; "
                              "DROP TABLE IF EXISTS QueriesDisks CASCADE; "
                              "DROP TABLE IF EXISTS RAMsDisks CASCADE; ")
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO Queries VALUES ("
                              "{queryID}, "
                              "{purpose}, "
                              "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                                purpose=sql.Literal(query.getPurpose()), \
                                                size=sql.Literal(query.getSize()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def getQueryProfile(queryID: int) -> Query:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT * FROM Queries WHERE queryID = {ID}".format(ID=queryID))
        rows_effected, result = conn.execute(transaction)
        ret = buildQuery(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = Query.badQuery()
    finally:
        conn.close()
        return ret


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("UPDATE Disks d "
                              "SET free_space = qdws.free_space + qdws.size "
                              "FROM QueriesDisksWithSizes qdws "
                              "WHERE qdws.queryID = {ID} AND qdws.diskID = d.diskID; "
                              "DELETE FROM Queries WHERE queryID = {ID}") \
            .format(ID=sql.Literal(query.getQueryID()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    except Exception as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return ret


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO Disks VALUES ("
                              "{diskID}, "
                              "{company}, "
                              "{speed}, "
                              "{free_space}, "
                              "{cost})").format(diskID=sql.Literal(disk.getDiskID()), \
                                                company=sql.Literal(disk.getCompany()), \
                                                speed=sql.Literal(disk.getSpeed()), \
                                                free_space=sql.Literal(disk.getFreeSpace()), \
                                                cost=sql.Literal(disk.getCost()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT * FROM Disks WHERE diskID = {ID}".format(ID=diskID))
        rows_effected, result = conn.execute(transaction)
        ret = buildDisk(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = Disk.badDisk()
    finally:
        conn.close()
        return ret


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DELETE FROM Disks WHERE diskID = {ID}").format(ID=sql.Literal(diskID))
        rows_effected, result = conn.execute(transaction)
        if rows_effected == 0:
            ret = ReturnValue.NOT_EXISTS
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO RAMs VALUES ("
                              "{ramID}, "
                              "{size}, "
                              "{company})").format(ramID=sql.Literal(ram.getRamID()), \
                                                   size=sql.Literal(ram.getSize()), \
                                                   company=sql.Literal(ram.getCompany()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT * FROM RAMs WHERE ramID = {ID}".format(ID=ramID))
        rows_effected, result = conn.execute(transaction)
        ret = buildRAM(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = RAM.badRAM()
    finally:
        conn.close()
        return ret


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DELETE FROM RAMs WHERE ramID = {ID}").format(ID=sql.Literal(ramID))
        rows_effected, result = conn.execute(transaction)
        if rows_effected == 0:
            ret = ReturnValue.NOT_EXISTS
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO Disks VALUES ("
                              "{diskID}, "
                              "{company}, "
                              "{speed}, "
                              "{free_space}, "
                              "{cost});"

                              "INSERT INTO Queries VALUES ("
                              "{queryID}, "
                              "{purpose}, "
                              "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                                purpose=sql.Literal(query.getPurpose()), \
                                                size=sql.Literal(query.getSize()), \
                                                diskID=sql.Literal(disk.getDiskID()), \
                                                company=sql.Literal(disk.getCompany()), \
                                                speed=sql.Literal(disk.getSpeed()), \
                                                free_space=sql.Literal(disk.getFreeSpace()), \
                                                cost=sql.Literal(disk.getCost()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
        conn.rollback()
    except Exception as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return ret


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO QueriesDisks(queryID, diskID) VALUES ("
                        "{queryID}, "
                        "{diskID});"
                        "UPDATE Disks "
                        "SET free_space=free_space-{size} "
                        "WHERE diskID={diskID}").format(queryID=sql.Literal(query.getQueryID()),
                                                        diskID=sql.Literal(diskID),
                                                        size=sql.Literal(query.getSize()))

        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        ret = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.database_ini_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    # TODO function gets no query id but whole query! Do i need to check all parameters?
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Disks "
                        "SET free_space=free_space+{size} "
                        "WHERE diskID={diskID};"
                        "DELETE FROM QueriesDisks "
                        "WHERE diskID={diskID} AND queryID={queryID} "
                        ).format(queryID=sql.Literal(query.getQueryID()),
                                 diskID=sql.Literal(diskID),
                                 size=sql.Literal(query.getSize()))

        rows_effected, _ = conn.execute(query)
        if rows_effected == 1:
            conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        ret = ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.OK
    except DatabaseException.database_ini_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.OK
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RamsDisks(ramID, diskID) VALUES ("
                        "{ramID}, "
                        "{diskID})").format(ramID=sql.Literal(ramID),
                                            diskID=sql.Literal(diskID))

        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        ret = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RamsDisks "
                        "WHERE diskID={diskID} AND ramID={ramID}; "
                        ).format(ramID=sql.Literal(ramID),
                                 diskID=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            ret = ReturnValue.NOT_EXISTS
        else:
            conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        ret = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.ERROR
    except DatabaseException.database_ini_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(AVG(Q.size), 0) AS avg "
                        "FROM Queries Q, (SELECT queryID "
                                          "FROM QueriesDisks "
                                          "WHERE diskID={diskID}) AS QD "
                        "WHERE Q.queryID = QD.queryID").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        ret = result[0]['avg']
        conn.commit()
        # TODO add exception: 0 in case of division by 0 or ID does not exist, -1 in case of other error.

    except DatabaseException.ConnectionInvalid as e:
        ret = -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        ret = -1
    except DatabaseException.CHECK_VIOLATION as e:
        ret = -1
    except DatabaseException.database_ini_ERROR as e:
        ret = -1
    except DatabaseException.UNKNOWN_ERROR as e:
        ret = -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = 0
    except Exception as e:
        ret = -1
    finally:
        conn.close()
        return ret


def diskTotalRAM(diskID: int) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(SUM(DRS.ramSum), 0) "
                        "FROM  DisksRAMSum DRS "
                        "WHERE DRS.diskID={diskID}").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        ret = result[0]['coalesce']
        conn.commit()
    except Exception as e:
        ret = -1
    finally:
        conn.close()
        return ret


def getCostForPurpose(purpose: str) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(SUM(D.cost*Q.size),0) "
                        "FROM Disks D, Queries Q, QueriesDisks AS QD "
                        "WHERE Q.purpose={purpose} AND Q.queryID=QD.queryID AND D.diskID=QD.diskID").format(
            purpose=sql.Literal(purpose))
        rows_effected, result = conn.execute(query)
        ret = result[0]['coalesce']
        conn.commit()
    except Exception as e:
        ret = -1
    finally:
        conn.close()
        return ret


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Q.queryID "
                        "FROM  (SELECT diskID, free_space FROM Disks WHERE diskID={diskID}) AS D, Queries Q "
                        "WHERE Q.size<=D.free_space "
                        "ORDER BY queryID DESC "
                        "LIMIT 5").format(
            diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        # TODO its repeat from averageSizeQueriesOnDisk

        for row in result:
            ret.append(row['queryID'])  # IT'S NOT A CALCULATION! JUST REARRANGING RETURN VALUE!
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = []
    finally:
        conn.close()
        return ret


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Q.queryID "
                        "FROM  (SELECT diskID, free_space FROM Disks WHERE diskID={diskID}) AS D, Queries Q, (SELECT COALESCE(SUM(DRS.ramSum), 0) AS ramSum FROM DisksRAMSum DRS WHERE diskID={diskID}) AS DRS "
                        "WHERE Q.size<=DRS.ramSum AND Q.size<=D.free_space "
                        "ORDER BY queryID ASC "
                        "LIMIT 5").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        # TODO its repeat from averageSizeQueriesOnDisk

        for row in result:
            if not row:
                break
            ret.append(row['queryID'])  # IT'S NOT A CALCULATION! JUST REARRANGING RETURN VALUE!
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = []
    except Exception as e:
        ret = []
    finally:
        conn.close()
        return ret


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = False
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT (COUNT(DISTINCT(RET.company))=1) AS res "
                        "FROM ((SELECT company FROM Disks WHERE diskID={diskID}) UNION (SELECT company FROM RAMs R, RAMsDisks RD WHERE RD.diskID={diskID} AND R.ramID=RD.ramID)) as RET ").format(
            diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        ret = result[0]['res']
        conn.commit()
    except Exception as e:
        ret = False
    finally:
        conn.close()
        return ret


def getConflictingDisks() -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT(ANS.diskID) AS ret "
                        "FROM (SELECT queryID FROM QueriesDisks GROUP BY queryID HAVING MIN(diskID)<> MAX(diskID)) AS QD "
                        "JOIN QueriesDisks AS ANS ON ANS.queryID = QD.queryID "
                        "ORDER BY ANS.diskID ASC")
        rows_effected, result = conn.execute(query)

        for row in result:
            if not row:
                break
            ret.append(row['ret'])  # IT'S NOT A CALCULATION! JUST REARRANGING RETURN VALUE!
        conn.commit()
    except Exception as e:
        ret = []
    finally:
        conn.close()
        return ret


####

def mostAvailableDisks() -> List[int]:
    conn = None
    ret = []
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT d.diskID, COALESCE(nonzero_query_count, 0) AS query_count, speed "
                              "FROM Disks d FULL JOIN (" #accounting for disks that can't run any queries
                                "SELECT diskID, COUNT(queryID) AS nonzero_query_count "
                                "FROM PossibleQueriesDisks "
                                "GROUP BY diskID ) pqd " #number of possible queries that can run on disk
                              "ON d.diskID = pqd.diskID "
                              "ORDER BY query_count DESC, speed DESC, diskID ASC "
                              "LIMIT 5")
        rows_effected, result = conn.execute(transaction)
        ret = buildAvailableDiskList(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = []
    finally:
        conn.close()
        return ret


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    ret = []
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT queryID "
                              "FROM ("
                                "SELECT q1.queryID, COALESCE(nonzero_same_count, 0) as same_count "
                                "FROM Queries q1 FULL JOIN (" #accounting for cases where a query isn't running on any disks
                                    "SELECT d1.queryID, COUNT(d1.diskID) AS nonzero_same_count "
                                    "FROM QueriesDisks d1, QueriesDisks d2 "
                                    "WHERE d1.queryID <> {ID} AND d1.diskID = d2.diskID AND d2.queryID = {ID}"
                                    "GROUP BY d1.queryID"
                                    ") q2 " #table that counts how many of the same disks as selected query each query is running on
                                "ON q1.queryID = q2.queryID "
                                "WHERE q1.queryID <> {ID}"
                                ") q3 " 
                              "WHERE same_count*2 >= ("
                              "SELECT COALESCE(COUNT(diskID), 0) as total_count "
                              "FROM QueriesDisks "
                              "WHERE queryID = {ID}) " #total disks selected is running on
                              "ORDER BY queryID ASC "
                              "LIMIT 10").format(ID=sql.Literal(queryID))
        rows_effected, result = conn.execute(transaction)
        ret = buildCloseQueriesList(result, rows_effected)
        conn.commit()
    except Exception as e:
        print(e)
        ret = []
    finally:
        conn.close()
        return ret

if __name__ == '__main__':
    print("Creating all tables")
    createTables()
    if addQuery(Query(-1, "something", 2)) != ReturnValue.BAD_PARAMS:
        print("Negative id error")
    if addQuery(Query(1, "something", -2)) != ReturnValue.BAD_PARAMS:
        print("Negative size error")
    if addQuery(Query(1, None, 2)) != ReturnValue.BAD_PARAMS:
        print("null values error")
    if addQuery(Query(1, "something", 2)) != ReturnValue.OK:
        print("add good query error")
    if addQuery(Query(1, "something else", 5)) != ReturnValue.ALREADY_EXISTS:
        print("duplicate query error")
    if getQueryProfile(1).getPurpose() != "something":
        print("get good query error")
    if getQueryProfile(0).getQueryID() != None:
        print("get non existent query error")
    if deleteQuery(Query(15, "something else", 5)) != ReturnValue.OK:
        print("delete query error")
    if addQuery(Query(2, "something else", 5)) != ReturnValue.OK:
        print("add good query error")
    if getQueryProfile(2).getPurpose() != "something else":
        print("get good query error")
    if deleteQuery(Query(2, "something else", 5)) != ReturnValue.OK:
        print("delete query error")
    if getQueryProfile(2).getQueryID() != None:
        print("get non existent query error")
    print("Clearing all tables")
    clearTables()
    if getQueryProfile(1).getQueryID() != None:
        print("clear tables error")
    print("Dropping all tables - empty database")
    dropTables()
