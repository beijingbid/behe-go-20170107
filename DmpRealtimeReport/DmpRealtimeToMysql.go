package main

//  localhost:8041/set?key=urlencode(BASE^2017-01-19^2^2^2^2)
import (
	"bufio"
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
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

var logFileName = flag.String("log", "debug.log", "Log file name")
var db *sql.DB
var pools_redis []*redis.Pool
var task_ch []chan string
var count int
var g_Config = make(map[string]string)

func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   32,
		MaxActive: 128, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func initMylog() {

}

func initConfig() {
	f, err := os.Open("db.conf")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		line = strings.Replace(line, "\r", "", -1)
		if line == "" {
			break
		}
		substr := line[0:1]
		if substr == "#" {
			continue
		}
		if strings.Contains(line, ":") {
			s := strings.Split(line, ":")
			g_Config[strings.Replace(s[0], " ", "", -1)] = strings.Replace(s[1], " ", "", -1)
		}

	}
	if g_Config["dbport"] == "" {
		g_Config["dbport"] = "3306"
	}
}

func initMysql() {

	//"dmpUser:5a0def138139a60f7a6d868e@(10.100.18.83:3306)/behe_yili_dmp_report_advert?charset=utf8"
	scfg := g_Config["dbuser"] + ":" + g_Config["dbpassword"] + "@(" + g_Config["dbhost"] + ":" + g_Config["dbport"] + ")/" + g_Config["dbname"] + "?charset=" + g_Config["dbcharset"]
	scfg = strings.Replace(scfg, "\n", "", -1)
	scfg = strings.Replace(scfg, "\r", "", -1)
	blog(scfg)
	var err error
	db, err = sql.Open("mysql", scfg)
	//db, err = sql.Open("mysql", "dmpUser:5a0def138139a60f7a6d868e@(10.100.18.83:3306)/behe_yili_dmp_report_advert?charset=utf8")
	if err != nil {
		blog("Connect Mysql err:", " err")
		return
	} else {
		blog("Connect Mysql OK!")
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	db.Ping()
}
func initRedis() {

	s := strings.Split(g_Config["redisServerList"], ",")
	for i := 0; i < len(s); i++ {
		sl := strings.Split(s[i], "|")
		if sl[1] == "" {
			sl[1] = "6379"
		}
		rcfg := sl[0] + ":" + sl[1]
		rcfg = strings.Replace(rcfg, "\n", "", -1)
		rcfg = strings.Replace(rcfg, "\r", "", -1)
		pools_redis = append(pools_redis, newPool(rcfg))
	}

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
	prara_map := make(map[string]string)
	paraURI(req.URL.String(), &prara_map)
	//只有符合格式规则，才加入task

	str_key_list_all := prara_map["key"]
	var strarr []string = strings.Split(str_key_list_all, ",")
	var strarr_sub []string
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
			idx := getCrcn(str_task_key, count)
			task_ch[idx] <- str_task_key
		}
	}
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

// 返回key的crc32
func getCrcn(key string, n int) int {
	crcv := int(getCrc(key))
	crcvs := strconv.Itoa(crcv)
	crcvsi, _ := strconv.Atoi(crcvs)
	if crcvsi < 0 {
		crcvsi = -1 * crcvsi
	}
	return crcvsi % n
}

func processTask(idx int) {
	for {
		str_task_key, ok := <-task_ch[idx]
		if ok == false {
			blog("task queue empty!")
		}
		Excute(str_task_key, idx)
	}
}

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
	timestamp, _ := strconv.Atoi(strarr[1])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05")

	redis_conn := pools_redis[0].Get()
	defer redis_conn.Close()
	task_rec, err := redis.StringMap(redis_conn.Do("HGETALL", str_task_key))
	if err != nil {
		blog("getTaskInfo Err:")
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

	del_sql := "delete from campaign_realtime WHERE 1 and reportDate = '" + str_today + "' and departmentId = '" + strarr[2] + "' and brandId = '" + strarr[3] + "' and productId = '" + strarr[4] + "' and campaignId = '" + strarr[5] + "';"
	insert_sql := "insert into campaign_realtime (`id`, `departmentId`, `brandId`, `productId`, `campaignId`, `reportDate`, `pv`, `click`) values(NULL,'" + strarr[3] + "','" + strarr[3] + "','" + strarr[4] + "','" + strarr[5] + "','" + str_today + "','" + view + "','" + click + "');"

	return del_sql, insert_sql, true

	return "", "", false
}

func excuteSql(str_sql string, idx int, str_task_key string) {
	if len(str_sql) <= 0 {
		fmt.Println("Thread id:", idx, ",Task:", str_task_key, ",Sql:", str_sql, ",Err: sql size < 0")
		return
	}
	stmt, err := db.Prepare(str_sql)

	defer stmt.Close()
	if stmt != nil || err != nil {
		stmt.Exec()
	} else {
		fmt.Println(" Err stmt:", stmt)
		fmt.Println(" Err err:", err)
	}

	return
}

func Excute(str_task_key string, idx int) {
	str_sql_del, str_sql_insert, isok := genSQL(str_task_key)

	if isok == false {
		blog("genSQL err ! ", str_task_key, str_sql_del, str_sql_insert)
		return
	}

	excuteSql(str_sql_del, idx, str_task_key)
	excuteSql(str_sql_insert, idx, str_task_key)
	return
}
func blog(str ...string) {

	logFile, logErr := os.OpenFile(*logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if logErr != nil {
		fmt.Println("Fail to find", *logFile, "cServer start Failed")
		os.Exit(1)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.SetPrefix("[Info]")
	//write log

	//timestamp := time.Now().Unix()
	//tm := time.Unix(timestamp, 0)
	//log.Printf(tm.Format("2006-01-02 03:04:05 PM "))
	for i := 0; i < len(str); i++ {
		log.Printf(" " + str[i])
	}
}

func main() {
	fmt.Println(" start")
	runtime.GOMAXPROCS(runtime.NumCPU())
	//initMylog()
	initConfig()
	initMysql()
	initRedis()
	count = 32
	blog(" progream start ")

	for i := 0; i < count; i++ {
		task_ch = append(task_ch, make(chan string, 300000))
	}

	for i := 0; i < count; i++ {
		go processTask(i)
	}
	http.Handle("/set", http.HandlerFunc(addTask))

	err := http.ListenAndServe(":8041", nil)
	if err != nil {
		blog("Err for http handle")
	}
	blog(" progream end ")
}
