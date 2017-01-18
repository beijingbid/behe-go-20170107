package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/garyburd/redigo/redis"
)

var (
	logFileName = flag.String("log", "cServer.log", "Log file name")
	pools_redis []*redis.Pool
)

func loaddata(str string) {

	blog(" func loaddata val:" + str + "\n")
	getredisvalue(str)
}

func getredisvalue(str string) {

	blog("func getredisvalue " + str)

	redis_conn := pools_redis[0].Get()

	res, err := redis.String(redis_conn.Do("GET", str))
	if err != nil {
		fmt.Fprintln(os.Stderr, "GET "+str, res, err)
	}
	redis_conn.Close()
	blog(" get redis [" + str + "] value=[")
	blog(res)
	blog("]" + strconv.Itoa(len(pools_redis)))

	blog(" call sql ")

	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/test?charset=utf8")
	//CheckErr(err)
	stmt, err := db.Prepare(`INSERT test (id1,key1,value1,status1)values (?,?,?,?)`)
	//checkErr(err)
	result, err := stmt.Exec(nil, str, res, nil)
	//checkErr(err)
	id, err := result.LastInsertId()
	//checkErr(err)
	//fmt.Println(id)
	blog("id = " + strconv.FormatInt(id, 10))
}

func geturlvalue(w http.ResponseWriter, r *http.Request) {
	r.ParseForm() //解析参数, 默认是不会解析的

	for k, v := range r.Form {
		if k == "key" {
			//fmt.Fprintf(w, "key:"+k+"\n")
			//fmt.Fprintf(w, "val:"+strings.Join(v, "")+"\n")
			blog("key:" + k + " " + "val:" + strings.Join(v, ""))
			loaddata(strings.Join(v, ""))
		}
	}
	fmt.Fprintf(w, "ok") //输出到客户端的信息

}

func main() {
	pools_redis = append(pools_redis, newPool("127.0.0.1:6379"), newPool("127.0.0.1:6379"))

	http.HandleFunc("/go/DmpRealtimeReport", geturlvalue) //设置访问的路由
	err := http.ListenAndServe(":9090", nil)              //设置监听的端口
	if err != nil {
		blog("ListenAndServe:")
		//log.Fatal("ListenAndServe: ", err)
	}
	blog("asdf")

}

func blog(str string) {

	logFile, logErr := os.OpenFile(*logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if logErr != nil {
		fmt.Println("Fail to find", *logFile, "cServer start Failed")
		os.Exit(1)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.SetPrefix("[Info]")
	//write log
	log.Printf(" " + str)
}

// 建立redis 连接
func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   32,
		MaxActive: 64, // max number of connections
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

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
