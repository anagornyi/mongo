// query_profiler.cpp

/**
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "query_profiler.h"

namespace mongo {
    
    // Wrapper class for data structure holding query classification data
    class QueryProfiles {
    public:
        static void addQuery(BSONObj& query) {
            std::map< BSONObj , int >::iterator it = _queryMap.find(query);
            if (it == _queryMap.end()) {
                _queryMap[query] = 1;
            } 
            else {
                int count = _queryMap[query];
                _queryMap[query] = ++count;
            }
        }
        static std::map< BSONObj , int > getQueryMap() {
            return _queryMap;
        }
    private:
        static std::map< BSONObj , int > createQueryMap() {
            std::map<BSONObj , int> qm;
            return qm;
        }
        static std::map< BSONObj , int > _queryMap;
    } queryProfiles;

    std::map< BSONObj , int > QueryProfiles::_queryMap(QueryProfiles::createQueryMap());
    
    // Database command to retrieve query classifications
    class CmdGetQueryProfiles : public Command {
    public:
        CmdGetQueryProfiles():Command("getQueryProfiles") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return true; };
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            std::map< BSONObj , int > qm = QueryProfiles::getQueryMap();
            BSONObjBuilder b;
            for( std::map< BSONObj , int >::iterator it = qm.begin(); it != qm.end(); ++it) {
                b.append(it->first.toString(), it->second);
            }
            result.append("profiles", b.obj());
            return true;
        }    
    } cmdGetQueryProfiles;
    
    // Retrieve and log query from currentOp
    void profileQueries(const Client& c, CurOp& currentOp) {
        if (!str::equals(opToString(currentOp.getOp()), "query")) {
            return;
        }
        BSONObj query = currentOp.query();
        BSONObj querySkeleton = getQuerySkeleton(query);
        QueryProfiles::addQuery(querySkeleton);
    }
    
    // Retrieve a query skeleton
    // For example: { x : 23 , y : { z : 12 } } --> { x : 1 , y : { z : 1 } }
    BSONObj getQuerySkeleton(BSONObj& query) {
        BSONObjBuilder result;
        BSONObjIteratorSorted it(query);
        BSONObj sub , tmpObj;	    
        BSONElement e;

        while (it.more()) {
            e = it.next();
            const char* fieldName = e.fieldName();
            
            // Account for queries using "." notation
            const char *p = strchr(fieldName, '.');
            if (p) {
                tmpObj = dotted2nested(e.wrap());
                string left(fieldName, p-fieldName);
                fieldName = left.c_str();
                sub = tmpObj.getObjectField(fieldName);
                result.append(StringData(fieldName), getQuerySkeleton(sub));
                continue;
            } 
            
            tmpObj = e.wrap();
            sub = tmpObj.getObjectField(fieldName);
            
            if (!sub.isEmpty()) {
                result.append(StringData(fieldName), getQuerySkeleton(sub));
            } 
            else {
                result.append(StringData(fieldName), 1);
            }
        }
        return result.obj();
    }
    
} //namespace mongo
