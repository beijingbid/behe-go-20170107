package main

//  localhost:8041/set?key=urlencode(BASE^2017-01-19^2^2^2^2)
import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
)

var logger_task *log.Logger
var logger_err *log.Logger
var db *sql.DB
var pools_redis []*redis.Pool
var task_ch []chan string
var count int

func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   32,
		MaxActive: 128, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				//panic(err.Error())
				fmt.Println(err.Error())
			}
			return c, err
		},
	}
}

func initmylog() {
	task_log_file := "./task.log"
	tasklogfile, err := os.OpenFile(task_log_file, os.O_RDWR|os.O_CREATE, 0)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}
	err_log_file := "./err.log"
	errlogfile, err := os.OpenFile(err_log_file, os.O_RDWR|os.O_CREATE, 0)
	if err != nil {
		fmt.Printf("%s\r\n", err.Error())
		os.Exit(-1)
	}
	logger_task = log.New(tasklogfile, "", 0)
	logger_err = log.New(errlogfile, "", 0)

}

func initmysql() {
	var err error
	db, err = sql.Open("mysql", "root:123456@(localhost:3306)/test?charset=utf8")
	if err != nil {
		fmt.Println("Connect Mysql err:", err)
		return
	} else {
		fmt.Println("Connect Mysql OK!")
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	db.Ping()
}

func paraURI(str_uri string, para_map *map[string]string) {
	ipos := strings.Index(str_uri, "?")
	var str_para []byte
	if ipos >= 0 {
		str_para = []byte(str_uri)[ipos+1:]
	}
	var strarr []string = strings.Split(string(str_para), "&")
	n := len(strarr)

	for i := 0; i < n; i++ {
		var str_para_filed []byte
		var str_para_x []byte
		xpos := strings.Index(strarr[i], "=")
		if xpos >= 0 {
			str_para_filed = []byte(strarr[i])[0:xpos]
			str_para_x = []byte(strarr[i])[xpos+1:]
			if string(str_para_filed) == "key" {
				(*para_map)[string(str_para_filed)] = string(str_para_x)
			}
		}
	}
}

func addTask(writer http.ResponseWriter, req *http.Request) {
	start_time := time.Now().UnixNano()

	prara_map := make(map[string]string)
	paraURI(req.URL.String(), &prara_map)
	//str_redis_key := prara_map["key"]
	//只有符合格式规则，才加入task

	str_key_list_all := prara_map["key"]
	var strarr []string = strings.Split(str_key_list_all, ",")
	var strarr_sub []string
	//var hash_value int
	n := len(strarr)
	for i := 0; i < n; i++ {
		task_key, _ := base64.StdEncoding.DecodeString(strarr[i])
		str_task_key := string(task_key)
		strarr_sub = strings.Split(str_task_key, "^")
		did, _ := strconv.Atoi(strarr_sub[2])
		bid, _ := strconv.Atoi(strarr_sub[3])
		pid, _ := strconv.Atoi(strarr_sub[4])
		cid, _ := strconv.Atoi(strarr_sub[5])
		if strarr_sub[0] == "BASE" && len(strarr_sub) == 6 && did > 0 && bid > 0 && pid > 0 && cid > 0 {
			hash_value := getCrc(str_task_key)
			idx := int(hash_value) % count
			task_ch[idx] <- str_task_key
		}
	}
	end_time := time.Now().UnixNano()
	fmt.Fprintln(writer, "ok use time:", (end_time-start_time)/1000000, "ms")
	return
}

// 返回key的crc32
func getCrc(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}
func processTask(idx int) {
	for {
		str_task_key, ok := <-task_ch[idx]
		if ok == false {
			fmt.Println("task queue empty!")
		}

		Excute(str_task_key, idx)
	}
}

/*func getTaskInfo(str_task_key string) (map[string]string),bool {
	redis_conn := pools_redis[0].Get()
	defer redis_conn.Close()
	task_rec,err:=redis.StringMap(redis_conn.Do("HGETALL",str_task_key))
	if err!=nil {
		fmt.Println("getTaskInfo Err:",click_rec,"HGETALL",str_task_key)
		return task_rec, false
	}
	return task_rec, true
}*/

func genSQL(str_task_key string) (string, string, bool) {
	var strarr []string = strings.Split(str_task_key, "^")
	if strarr[0] != "BASE" {
		return "", "", false
	}
	if len(strarr) < 5 {
		return "", "", false
	}
	if (len(strarr[2]) <= 0) || (len(strarr[3]) <= 0) || (len(strarr[4]) <= 0) || (len(strarr[5]) <= 0) {
		return "", "", false
	}
	if (strarr[2] == "0") || (strarr[3] == "0") || (strarr[4] == "0") || (strarr[5] == "0") {
		return "", "", false
	}
	/*task_rec,isok:=getTaskInfo(str_task_key)
	if isok ==false{
		return "","",false
	}*/
	redis_conn := pools_redis[0].Get()
	defer redis_conn.Close()
	task_rec, err := redis.StringMap(redis_conn.Do("HGETALL", str_task_key))
	if err != nil {
		fmt.Println("getTaskInfo Err:", err, "HGETALL", str_task_key)
		return "", "", false
	}

	view := task_rec["view"]
	if len(view) <= 0 {
		view = "0"
	}

	click := task_rec["click"]
	if len(click) <= 0 {
		click = "0"
	}

	var date_arr []string = strings.Split(strarr[1], "-")
	if len(date_arr) != 3 {
		return "", "", false
	}

	del_sql := "delete from campaign_realtime WHERE 1 and reportDate = '" + strarr[1] + "' and departmentId = '" + strarr[2] + "' and brandId = '" + strarr[3] + "' and productId = '" + strarr[4] + "' and campaignId = '" + strarr[5] + "';"
	insert_sql := "insert into campaign_realtime (`id`, `departmentId`, `brandId`, `productId`, `campaignId`, `reportDate`, `pv`, `click`) values(NULL,'" + strarr[2] + "','" + strarr[3] + "','" + strarr[4] + "','" + strarr[5] + "','" + strarr[1] + "','" + view + "','" + click + "')"

	return del_sql, insert_sql, true

	return "", "", false
}

func excuteSql(str_sql string, idx int, str_task_key string) {
	if len(str_sql) <= 0 {
		logger_err.Println("Thread id:", idx, ",Task:", str_task_key, ",Sql:", str_sql, ",Err: sql size < 0")
		return
	}
	stmt, err := db.Prepare(str_sql)
	if err != nil {
		logger_err.Println("Thread id:", idx, ",Task:", str_task_key, ",Sql:", str_sql, ",Err:", err)
		return
	}
	defer stmt.Close()
	res, err_r := stmt.Exec()
	if err_r != nil {
		logger_err.Println("Thread id:", idx, ",Task:", str_task_key, ",Sql:", str_sql, ",Err:", err_r)
		return
	}
	logger_task.Println("Thread id:", idx, ",Task:", str_task_key, ",Sql:", str_sql, ",Res:", res)
	return
}

func Excute(str_task_key string, idx int) {
	str_sql_del, str_sql_insert, isok := genSQL(str_task_key)
	if isok == false {
		//logger_err.Println("genSQL err ! ",str_task_key,str_sql_del, str_sql_insert, isok, str_sql_del2, str_sql_insert2)
		return
	}

	//fmt.Println("Thread id:",idx,",Task:",str_task_key,",SqlDel:",str_sql_del)
	//fmt.Println("Thread id:",idx,",Task:",str_task_key,",SqlIns:",str_sql_insert)
	excuteSql(str_sql_del, idx, str_task_key)
	excuteSql(str_sql_insert, idx, str_task_key)
	return
}

func main() {
	pools_redis = append(pools_redis, newPool("127.0.0.1:6379"), newPool("127.0.0.1:6379"))
	runtime.GOMAXPROCS(runtime.NumCPU())
	initmylog()
	initmysql()
	count = 32

	for i := 0; i < count; i++ {
		task_ch = append(task_ch, make(chan string, 300000))
	}

	for i := 0; i < count; i++ {
		go processTask(i)
	}
	http.Handle("/set", http.HandlerFunc(addTask))

	err := http.ListenAndServe(":8041", nil)
	if err != nil {
		fmt.Println("Err", err)
	}
}
