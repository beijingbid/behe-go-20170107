package main

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	//"encoding/json"
	"crypto/rand"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	//"os/signal"
	//"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	//kafka "github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
)

type reportInfo struct {
	did string
	bid string
	pid string
	cid string

	view_num  int64
	click_num int64
	m_lock    sync.Mutex
}

var log_resource string
var logFileName = flag.String("log", "DmpReaitimeReport.log", "Log file name")

type RecoredSet struct {
	m_record      map[string]*reportInfo
	m_record_lock sync.RWMutex
}

var sep = '\x02'
var sep_str = string(sep)

var g_recored []RecoredSet
var shift_g_recored_lock sync.RWMutex
var rec_idx int
var recordset_size int

var task_ch []chan string
var count int

var pools_redis []*redis.Pool
var pools_redis_click []*redis.Pool

var conn_http *http.Client = &http.Client{

	Transport: &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			conn_http, err := net.DialTimeout(netw, addr, time.Second*1)
			if err != nil {
				fmt.Println("dail timeout", err)
				return nil, err
			}
			conn_http.SetDeadline(time.Now().Add(time.Second * 15))
			return conn_http, nil

		},
		MaxIdleConnsPerHost:   64,
		ResponseHeaderTimeout: time.Millisecond * 40,
		DisableKeepAlives:     false,
	},
}

// 发请求的方法，可以留用
func requestHttp(str_task_key string) bool {

	hv := getCrc(str_task_key)
	str_hv := strconv.Itoa(int(hv))
	str_b64 := base64.StdEncoding.EncodeToString([]byte(str_task_key))
	str_url := "http://localhost:8041/set?hv=" + str_hv + "&key=" + str_b64
	resp, err := conn_http.Get(str_url)
	if err != nil {
		//blog(" ERR1:")
		fmt.Println("Err1:", err)
		return false
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//blog("Err2:")
		fmt.Println("Err2:", err, "body:", body)
		return false
	}
	return true
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
			(*para_map)[string(str_para_filed)] = string(str_para_x)
		}
	}
}

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

func getMd5(key string) string {
	h := md5.New()
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}

func initRecoredSet() {
	//blog(" func initRecoredSet start")
	var v RecoredSet
	var v1 RecoredSet
	v.m_record = make(map[string]*reportInfo)
	v1.m_record = make(map[string]*reportInfo)
	g_recored = append(g_recored, v)
	g_recored = append(g_recored, v1)
	rec_idx = 0
	recordset_size = 2
	//blog(" func initRecoredSet end")
}

func getCrc(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

// 管道数量
func initTaskCh() {
	//blog(" func initTaskCh start")
	count = 32
	for i := 0; i < count; i++ {
		task_ch = append(task_ch, make(chan string, 300000))
		//blog(" run task_ch " + strconv.Itoa(i))
	}
	//blog(" func initTaskCh end")
}
func create_demo() string {

	rn := RandInt64(3, 5)
	timestamp := time.Now().Unix()

	demo_log := "0"
	if rn == 3 {
		demo_log += sep_str + "3"

	} else if rn == 5 {
		demo_log += sep_str + "5"

	} else if rn == 4 {
		demo_log += sep_str + "4"

	} else {
		demo_log += sep_str + "4"
	}
	demo_log += sep_str + strconv.FormatInt(timestamp, 10)
	demo_log += sep_str + strconv.FormatInt(RandInt64(111, 115), 10)
	demo_log += sep_str + strconv.FormatInt(RandInt64(111, 115), 10)
	demo_log += sep_str + strconv.FormatInt(RandInt64(111, 115), 10)
	demo_log += sep_str + strconv.FormatInt(RandInt64(111, 115), 10)
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	demo_log += sep_str + " "
	////blog(" demo_log " + demo_log)
	return demo_log
}

func fillTask_syslog() {
	//blog(" func fillTask_syslog start")
	cmd := exec.Command("cat", "dsp_pipe_count")
	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	inputReader := bufio.NewReader(stdout)

	for {

		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			/*
				time.Sleep(1 * time.Second)
				stdout.Close()
				cmd = exec.Command("cat", "dsp_pipe_count")
				stdout, _ = cmd.StdoutPipe()
				cmd.Start()
				inputReader = bufio.NewReader(stdout)
				continue*/
			time.Sleep(1 * time.Second)
			inputString = create_demo()
			//blog(" call func create_demo and result = " + inputString)
		}
		task_ch[getCrc(inputString)%uint32(count)] <- inputString
	}
	//blog(" func fillTask_syslog end")
}

func processTask(idx int) {
	//blog(" func processTask start")
	for {
		str_log, ok := <-task_ch[idx]
		if ok == false {
			//blog("task queue empty!")
			fmt.Println("task queue empty!")
		}
		if str_log == "" {
			str_log = create_demo()
		}
		////blog(" call func Excute [" + str_log + "],[" + strconv.Itoa(idx) + "]")
		Excute(str_log, idx)
	}
	//blog(" func processTask end")
}

func Excute(str_log string, idx int) {
	str_log = strings.Replace(str_log, "\n", "", 1)
	str_log = strings.Replace(str_log, "\r", "", 1)
	str_log = strings.Replace(str_log, "\t", "", 1)
	////blog(" Excute log " + str_log)
	var log_arr []string = strings.Split(str_log, sep_str)
	if len(log_arr) > 2 {
		if (log_arr[1] == "4") && (len(log_arr) >= 15) {
			click_process(&log_arr)
		} else if (log_arr[1] == "3") && (len(log_arr) >= 15) {
			view_process(&log_arr)
		} else if (log_arr[1] == "5") && (len(log_arr) >= 15) {
			view_process(&log_arr)
		}
		////blog(" func Excute " + log_arr[1])
	}
}

func viewSetRecord(strkey, did, bid, pid, cid string) {
	var tmprec *reportInfo
	g_recored[rec_idx].m_record_lock.RLock()
	tmprec = g_recored[rec_idx].m_record[strkey]
	g_recored[rec_idx].m_record_lock.RUnlock()
	if tmprec == nil {
		var x reportInfo
		g_recored[rec_idx].m_record_lock.Lock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		if tmprec == nil {
			g_recored[rec_idx].m_record[strkey] = &x
			tmprec = &x
		}
		g_recored[rec_idx].m_record_lock.Unlock()
	} else {
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		g_recored[rec_idx].m_record_lock.RUnlock()
	}
	tmprec.m_lock.Lock()
	tmprec.did = did
	tmprec.bid = bid
	tmprec.pid = pid
	tmprec.cid = cid
	tmprec.view_num++
	tmprec.m_lock.Unlock()
	//blog(" view did " + did + " bid " + bid + " pid " + pid + " cid " + cid + " view " + strconv.FormatInt(tmprec.view_num, 10))

}

func view_process(strarr *[]string) {
	did := (*strarr)[3]
	bid := (*strarr)[4]
	pid := (*strarr)[5]
	cid := (*strarr)[6]

	timestamp, _ := strconv.Atoi((*strarr)[2])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	//str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	//str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())
	if str_today == "1970-01-01" {

	} else {
		shift_g_recored_lock.RLock()

		//基础报表-------------------
		tb_base_key := "BASE^" + str_today + "^" + did + "^" + bid + "^" + pid + "^" + cid
		viewSetRecord(tb_base_key, did, bid, pid, cid)

		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}

}

/*
func record_Click(did , bid , pid ,cid string) int {

	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()
	res, err := redis.Int(redis_conn_click.Do("HSETNX", bhuid, order_id, clickinfo))
	if err != nil {
		fmt.Println("ErrHs:", res, "HSETNX", bhuid, order_id, clickinfo)
	}
	//fmt.Println("HSET",bhuid,order_id,clickinfo)
	if res == 0 {
		res_n, err_n := redis.Int(redis_conn_click.Do("HSET", bhuid, order_id, (clickinfo + "|1")))
		if err_n != nil {
			fmt.Println("ErrHs:", res_n, "HSET", bhuid, order_id, clickinfo)
		}
	}
	now_time := time.Now().Unix()
	today := (now_time+28800)/86400*86400 - 28800
	res_exp, err := redis_conn_click.Do("EXPIREAT", bhuid, today+86399)
	if err != nil {
		fmt.Println("ErrHs:", res_exp, "EXPIREAT", bhuid, today+86399)
	}
	return res
}

*/
func clickSetRecord(strkey string, did string, pid string, bid string, cid string) {
	//blog(" debug func clickSetRecord ")
	var tmprec *reportInfo
	g_recored[rec_idx].m_record_lock.RLock()
	tmprec = g_recored[rec_idx].m_record[strkey]
	g_recored[rec_idx].m_record_lock.RUnlock()
	if tmprec == nil {
		//blog(" debug func clickSetRecord step 1")
		var x reportInfo
		g_recored[rec_idx].m_record_lock.Lock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		if tmprec == nil {
			g_recored[rec_idx].m_record[strkey] = &x
			tmprec = &x
		}
		g_recored[rec_idx].m_record_lock.Unlock()
	} else {
		//blog(" debug func clickSetRecord step 2")
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		g_recored[rec_idx].m_record_lock.RUnlock()
	}
	//blog(" debug func clickSetRecord step 3")
	tmprec.m_lock.Lock()
	tmprec.did = did
	tmprec.bid = bid
	tmprec.pid = pid
	tmprec.cid = cid
	tmprec.click_num++
	tmprec.m_lock.Unlock()
	//blog(" debug func clickSetRecord step 4")
	//blog(" click did " + did + " bid " + bid + " pid " + pid + " cid " + cid + " view " + strconv.FormatInt(tmprec.click_num, 10))

}

func click_process(strarr *[]string) {
	did := (*strarr)[3]
	bid := (*strarr)[4]
	pid := (*strarr)[5]
	cid := (*strarr)[6]
	timestamp, _ := strconv.Atoi((*strarr)[2])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	//str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	//str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())
	if str_today == "1970-01-01" {

	} else {
		shift_g_recored_lock.RLock()
		tb_base_key := "BASE^" + str_today + "^" + did + "^" + bid + "^" + pid + "^" + cid
		clickSetRecord(tb_base_key, did, bid, pid, cid)
		shift_g_recored_lock.RUnlock()
	}
}

/*
func raSetRecord(strkey , did,bid,pid,cid string) {
	var tmprec *reportInfo
	g_recored[rec_idx].m_record_lock.RLock()
	tmprec = g_recored[rec_idx].m_record[strkey]
	g_recored[rec_idx].m_record_lock.RUnlock()
	if tmprec == nil {
		var x reportInfo
		g_recored[rec_idx].m_record_lock.Lock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		if tmprec == nil {
			x.action_map = make(map[string]int64)
			x.user_action_map = make(map[string]int64)
			g_recored[rec_idx].m_record[strkey] = &x
			tmprec = &x
		}
		g_recored[rec_idx].m_record_lock.Unlock()
	} else {
		g_recored[rec_idx].m_record_lock.RLock()
		tmprec = g_recored[rec_idx].m_record[strkey]
		g_recored[rec_idx].m_record_lock.RUnlock()
	}
	tmprec.m_lock.Lock()
	tmprec.did = did
	tmprec.bid = bid
	tmprec.pid = pid
	tmprec.cid = cid
	tmprec.m_lock.Unlock()
}
*/
// 执行一次写入redis
func updateRecord() {

	//blog(" func updateRecord start")
	record_timestamp := time.Now().Unix()
	for {
		time.Sleep(30 * time.Second)
		old_rec_idx := rec_idx
		shift_g_recored_lock.Lock()
		if rec_idx == 0 {
			rec_idx = 1
		} else {
			rec_idx = 0
		}
		shift_g_recored_lock.Unlock()
		time_sep := time.Now().Unix() - record_timestamp
		record_timestamp = time.Now().Unix()
		updateMap2Redis(old_rec_idx, time_sep)

	}
	//blog(" func updateRecord end")
}

func updateMap2Redis(idx int, rec_time int64) {
	//blog(" func updateMap2Redis start")
	redis_conn := pools_redis[idx].Get()
	defer redis_conn.Close()
	//now_timestamp := time.Now().Unix()
	for redis_key, rpinfo := range g_recored[idx].m_record {
		//var strarr []string = strings.Split(redis_key, "^")
		/*res, err := redis_conn.Do("HMSET", redis_key, "view_num")
		if err != nil {
			//blog("ErrHs:")
			fmt.Println("ErrHs:", res, "HMSET", redis_key, "view_num")
		}
		res, err = redis_conn.Do("HMSET", redis_key, "click_num")
		//res, err := redis_conn.Do("HMSET", redis_key, "did", rpinfo.did, "bid", rpinfo.bid, "pid", rpinfo.pid, "cid", rpinfo.cid)
		if err != nil {
			//blog("ErrHs:")
			fmt.Println("ErrHs:", res, "HMSET", redis_key, "click_num")
		}*/
		res, err := redis_conn.Do("EXPIRE", redis_key, 172800)
		if err != nil {
			//blog("ErrHs:")
			fmt.Println("ErrHs: ", res, "EXPIRE", redis_key, 172800)
		}

		var tmp_num int64
		tmp_num = 0
		if rpinfo.view_num > 0 {
			//blog(" debug view_num = " + strconv.FormatInt(rpinfo.view_num, 10))
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "view", rpinfo.view_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "view", rpinfo.view_num)
			}
		}
		if rpinfo.click_num > 0 {
			//blog(" debug click_num = " + strconv.FormatInt(rpinfo.click_num, 10))
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "click", rpinfo.click_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "click", rpinfo.click_num)
			}
		} else {
			//blog(" debug click_num = 0")
		}
		// 发起一个http的请求
		requestHttp(redis_key)
	}
	g_recored[idx].m_record = make(map[string]*reportInfo)
	//blog(" func updateMap2Redis end")
}

func isSameDay(timestamp1 int, timestamp2 int) bool {
	if (timestamp1 == 0) || (timestamp2 == 0) {
		return true
	}
	str_day1 := time.Unix(int64(timestamp1), 0).Format("2006-01-02")
	str_day2 := time.Unix(int64(timestamp2), 0).Format("2006-01-02")
	if str_day1 == str_day2 {
		return true
	} else {
		return false
	}
}
func RandInt64(min, max int64) int64 {
	maxBigInt := big.NewInt(max)
	i, _ := rand.Int(rand.Reader, maxBigInt)
	iInt64 := i.Int64()
	if iInt64 < min {
		iInt64 = RandInt64(min, max) //应该用参数接一下
	}
	return iInt64
}

// 休眠时间
func loadsavetask() {
	for {
		loadfile()
		time.Sleep(60 * time.Second)
	}
}

//发送保存数据的任务
func loadfile() {
	//blog(" func loadfile")
	/*
		blacklistip_map_lock.Lock()
		blacklistip_map = make(map[string]int)
		blacklistip_map_lock.Unlock()*/
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

func main() {
	blog(" func main start")
	go loadsavetask()
	flag.Parse()
	//log_resource := flag.Arg(1)
	pools_redis = append(pools_redis, newPool("127.0.0.1:6379"), newPool("127.0.0.1:6379"))
	pools_redis_click = append(pools_redis_click, newPool("127.0.0.1:6379"), newPool("127.0.0.1:6379"))

	runtime.GOMAXPROCS(runtime.NumCPU())
	var quit chan int
	initRecoredSet()
	initTaskCh()
	go fillTask_syslog()
	/*if log_resource == "syslog" {

	} else {
		//go fillTask_kafka()
	}*/
	for i := 0; i < count; i++ {
		go processTask(i)
	}
	go updateRecord()

	blog(" func main end")
	<-quit
}
