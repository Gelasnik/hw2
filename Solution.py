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
    return Disk(result[0]['diskID'], result[0]['company'], result[0]['speed'], result[0]['free_space'],
                result[0]['cost'])


def buildRAM(result: ResultSet, rows_effected: int) -> RAM:
    if rows_effected != 1:
        raise Exception()
    return RAM(result[0]['ramID'], result[0]['company'], result[0]['size'])


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
            "ON DELETE CASCADE "
            "); "

            "CREATE TABLE RAMsDisks ("
            "ramID INTEGER, "
            "diskID INTEGER, "
            "FOREIGN KEY (ramID) "
            "REFERENCES RAMs(ramID) "
            "ON DELETE CASCADE, "
            "FOREIGN KEY (diskID) "
            "REFERENCES Disks(diskID) "
            "ON DELETE CASCADE "
            ");"

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
        query = sql.SQL("DELETE FROM Queries; "
                        "DELETE FROM Disks; "
                        "DELETE FROM RAMs;")
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


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DROP TABLE IF EXISTS Queries CASCADE; "
                        "DROP TABLE IF EXISTS Disks CASCADE; "
                        "DROP TABLE IF EXISTS RAMs CASCADE; "
                        "DROP TABLE IF EXISTS QueriesDisks CASCADE; "
                        "DROP TABLE IF EXISTS RAMsDisks CASCADE; ")
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


def addQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Queries VALUES ("
                        "{queryID}, "
                        "{purpose}, "
                        "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                          purpose=sql.Literal(query.getPurpose()), \
                                          size=sql.Literal(query.getSize()))
        conn.execute(query)
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
        query = sql.SQL("SELECT * FROM Queries WHERE queryID = {ID}".format(ID=queryID))
        rows_effected, result = conn.execute(query)
        ret = buildQuery(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = Query.badQuery()
    finally:
        conn.close()
        return ret


# TODO: Adjust free space on disk when deleting query. DOES NOT YET DO THIS
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Queries WHERE queryID = {ID}").format(ID=sql.Literal(query.getQueryID()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disks VALUES ("
                        "{diskID}, "
                        "{company}, "
                        "{speed}, "
                        "{free_space}, "
                        "{cost})").format(diskID=sql.Literal(disk.getDiskID()), \
                                          company=sql.Literal(disk.getCompany()), \
                                          speed=sql.Literal(disk.getSpeed()), \
                                          free_space=sql.Literal(disk.getFreeSpace()), \
                                          cost=sql.Literal(disk.getCost()))
        conn.execute(query)
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
        query = sql.SQL("SELECT * FROM Disks WHERE diskID = {ID}".format(ID=diskID))
        rows_effected, result = conn.execute(query)
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
        query = sql.SQL("DELETE FROM Disks WHERE diskID = {ID}").format(ID=sql.Literal(diskID))
        conn.execute(query)
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
        query = sql.SQL("INSERT INTO RAMs VALUES ("
                        "{ramID}, "
                        "{size}, "
                        "{company})").format(ramID=sql.Literal(ram.getRamID()), \
                                             size=sql.Literal(ram.getSize()), \
                                             company=sql.Literal(ram.getCompany()))
        conn.execute(query)
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
        query = sql.SQL("SELECT * FROM RAMs WHERE ramID = {ID}".format(ID=ramID))
        rows_effected, result = conn.execute(query)
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
        query = sql.SQL("DELETE FROM RAMs WHERE ramID = {ID}").format(ID=sql.Literal(ramID))
        conn.execute(query)
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
        query = sql.SQL("INSERT INTO Disks VALUES ("
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
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
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
        # TODO check if table checks free space existence
        conn.execute(query)
        conn.commit()
    # TODO add NOT_EXISTS exception
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


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM QueriesDisks "
                        "WHERE diskID={diskID} AND queryID={queryID}; "
                        "UPDATE Disks "
                        "SET free_space=free_space+{size} "
                        "WHERE diskID={diskID}").format(queryID=sql.Literal(query.getQueryID()),
                                                        diskID=sql.Literal(diskID))
        # TODO check if table checks free space existence
        conn.execute(query)
        conn.commit()
    # TODO add NOT_EXISTS exception
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
    # TODO add NOT_EXISTS exception
    except DatabaseException.ConnectionInvalid as e:
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
        conn.execute(query)
        conn.commit()
    # TODO add NOT_EXISTS exception
    except DatabaseException.ConnectionInvalid as e:
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
        query = sql.SQL("SELECT AVG(Q.size) "
                        "FROM Queries Q, (SELECT queryID "
                                             "FROM QueriesDisks "
                                             "WHERE diskID={diskID}) AS QD "
                        "WHERE Q.queryID = QD.queryID").format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        ret = result[0]['avg']
        conn.commit()
        # TODO add exception: 0 in case of division by 0 or ID does not exist, -1 in case of other error.
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
                        "WHERE Q.size<=D.free_space AND Q.queryID NOT IN (SELECT queryID FROM QueriesDisks WHERE diskID={diskID}) "
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
# ("SELECT Q.queryID "
#                         "FROM  (SELECT diskID, free_space FROM Disks WHERE diskID={diskID}) AS D, Queries Q "
#                         "WHERE Q.size<=D.free_space AND Q.queryID NOT IN (SELECT queryID FROM QueriesDisks WHERE diskID={diskID}) "
#                         "ORDER BY queryID DESC "
#                         "LIMIT 5")

def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    rows_effected, result = 0, ResultSet()
    ret = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Q.queryID "
                        "FROM  (SELECT diskID, free_space FROM Disks WHERE diskID={diskID}) AS D, Queries Q, (SELECT DRS.ramSum FROM DisksRAMSum DRS WHERE diskID={diskID}) AS DRS "
                        "WHERE Q.size<=DRS.ramSum AND Q.size<=D.free_space AND Q.queryID NOT IN (SELECT queryID FROM QueriesDisks WHERE diskID={diskID}) "
                        "ORDER BY queryID DESC "
                        "LIMIT 5").format(diskID=sql.Literal(diskID))
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


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []

####

def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []


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
