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
	"os/signal"
	//"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	kafka "github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
)

//topic list
var serverMap = make(map[int]string)
var serverid string
var g_Config = make(map[string]string)

var (
	configFile = flag.String("configfile", "dmpserver.conf", "General configuration file")
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

var logger_task *log.Logger
var logger_err *log.Logger
var log_resource string
var logFileName = flag.String("log", "debug.log", "Log file name")

type RecoredSet struct {
	m_record      map[string]*reportInfo
	m_record_lock sync.RWMutex
}

//var sep = '\x02'
var sep = ','
var sep_str = string(sep)

var g_recored []RecoredSet
var shift_g_recored_lock sync.RWMutex
var rec_idx int
var recordset_size int

var task_ch []chan string
var count int

var pools_redis []*redis.Pool

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

}

func initKafkaConfig() {
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

// 发请求的方法，可以留用
func requestHttp(str_task_key string) bool {

	hv := getCrc(str_task_key)
	str_hv := strconv.Itoa(int(hv))
	str_b64 := base64.StdEncoding.EncodeToString([]byte(str_task_key))
	str_url := "http://localhost:8041/set?hv=" + str_hv + "&key=" + str_b64
	resp, err := conn_http.Get(str_url)
	if err != nil {
		fmt.Println("Err1:", err)
		return false
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
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
	var v RecoredSet
	var v1 RecoredSet
	v.m_record = make(map[string]*reportInfo)
	v1.m_record = make(map[string]*reportInfo)
	g_recored = append(g_recored, v)
	g_recored = append(g_recored, v1)
	rec_idx = 0
	recordset_size = 2
}

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

// 管道数量
func initTaskCh() {
	count = 32
	for i := 0; i < count; i++ {
		task_ch = append(task_ch, make(chan string, 300000))
	}
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
	//blog(" demo_log " + demo_log)
	return demo_log
}

func fillTask_syslog() {
	cmd := exec.Command("cat", "/data/monitorlogs/monitor_pipe_report")
	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	inputReader := bufio.NewReader(stdout)

	for {

		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {

			time.Sleep(1 * time.Second)
			stdout.Close()
			cmd = exec.Command("cat", "monitor_pipe_report")
			stdout, _ = cmd.StdoutPipe()
			cmd.Start()
			inputReader = bufio.NewReader(stdout)
			continue
		}
		inputString = formatLog(inputString)
		idx := getCrcn(inputString, count)

		task_ch[idx] <- inputString
	}
}

// kafka
func fillTask_kafka() {

	kafkaServerId := g_Config["kafkaServerId"]
	s := strings.Split(g_Config["kafkaServerList"], ",")
	str_kafkaServerList := ""
	for i := 0; i < len(s); i++ {
		str_kafkaServerList += "\""
		serverMap[i] = s[i]
		s2 := strings.Split(s[i], "|")
		str_kafkaServerList += s2[0] + ":"
		if len(s2) == 1 || s2[1] == "" {
			str_kafkaServerList += "9092"
			//serverMap[i] = s2[0] + ":9092"
		} else {
			str_kafkaServerList += s2[1]
			//serverMap[i] = s2[0] + ":" + s2[1]
		}
		str_kafkaServerList += "\""
	}

	str_kafkaServerList = strings.Replace(str_kafkaServerList, "\n", "", 1)
	str_kafkaServerList = strings.Replace(str_kafkaServerList, "\r", "", 1)
	str_kafkaServerList = strings.Replace(str_kafkaServerList, "\t", "", 1)
	//consumer, err := kafka.NewConsumer([]string{"kafka-0001:9092", "kafka-0002:9092", "kafka-0003:9092", "kafka-0004:9092", "kafka-0005:9092", "kafka-0006:9092", "kafka-0007:9092", "kafka-0008:9092"}, nil)
	consumer, err := kafka.NewConsumer([]string{str_kafkaServerList}, nil)
	//consumer, err := kafka.NewConsumer([]string{"localhost:9092"}, nil)
	blog(" debug kafka server list = " + str_kafkaServerList)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("test", 0, kafka.OffsetNewest)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	blog(" debug kafkaserverid = " + kafkaServerId)
	blog(" debug len serverMap = " + fmt.Sprintf("%d", len(serverMap)))

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			//fmt.Println("Consumed message offset ", msg.Offset, string(msg.Value))
			blog(" debug get kafka inputString " + string(msg.Value))
			str_log := formatKafkaLog(string(msg.Value))
			blog(" debug get kafka formatLog " + str_log)
			// 这里 做cid 哈希的判断
			var log_arr []string = strings.Split(str_log, sep_str)
			if len(log_arr) < 9 {
				blog(" debug log_arr count = " + fmt.Sprintf("%d", len(log_arr)))
			} else {
				blog(" debug log 5 = " + log_arr[5])
				blog(" debug getcrc = " + fmt.Sprintf("%d", getCrc(log_arr[5])))
				s_idx := int(getCrc(log_arr[5]) % uint32(len(serverMap)))
				blog(" debug s_idx = " + fmt.Sprintf("%d", s_idx))
				blog(" debug serverMap[s_idx] = " + serverMap[s_idx])
				blog(" debug kafkaServerId = " + kafkaServerId)
				if serverMap[s_idx] == kafkaServerId {
					idx := getCrc(str_log) % uint32(count)
					task_ch[idx] <- str_log
				}
			}
			//task_ch[getCrc(string(msg.Value))%uint32(count)] <- string(msg.Value)
		case <-signals:
			break ConsumerLoop
		}
	}

	/*
		//#################################################

		   ConsumerLoop:
		   	for {
		   		select {
		   		case msg := <-partitionConsumer.Messages():
		   			blog(" get kafka inputString " + string(msg.Value))

		   			//fmt.Println("Consumed message offset ", msg.Offset, string(msg.Value))
		   			str_log := formatLog(string(msg.Value))
		   			// 这里 做cid 哈希的判断
		   			var log_arr []string = strings.Split(str_log, sep_str)
		   			s_idx := int(getCrc(log_arr[5]) % uint32(len(serverMap)))
		   			if serverMap[s_idx] == serverid {
		   				idx := getCrc(str_log) % uint32(count)
		   				task_ch[idx] <- str_log
		   			}
		   			//
		   		case <-signals:
		   			break ConsumerLoop
		   		}
		   	}*/
}

func processTask(idx int) {
	for {
		str_log, ok := <-task_ch[idx]
		if ok == false {
			fmt.Println("task queue empty!")
		} else {
			Excute(str_log, idx)
		}
	}
}

// 格式化日志，去掉日志的头部信息
func formatLog(str string) string {
	if strings.Index(str, "[INFO]") > 0 {
		return Substr(str, strings.Index(str, "[INFO]")+7, strings.Count(str, ""))
	} else {
		return Substr(str, 28, strings.Count(str, ""))
		//return str
	}

}
func formatKafkaLog(str string) string {
	return str

}

func Substr(str string, start, length int) string {
	rs := []rune(str)
	rl := len(rs)
	end := 0

	if start < 0 {
		start = rl - 1 + start
	}
	end = start + length

	if start > end {
		start, end = end, start
	}

	if start < 0 {
		start = 0
	}
	if start > rl {
		start = rl
	}
	if end < 0 {
		end = 0
	}
	if end > rl {
		end = rl
	}
	return string(rs[start:end])
}
func Excute(str_log string, idx int) {
	str_log = strings.Replace(str_log, "\n", "", 1)
	str_log = strings.Replace(str_log, "\r", "", 1)
	str_log = strings.Replace(str_log, "\t", "", 1)

	var log_arr []string = strings.Split(str_log, sep_str)
	if len(log_arr) > 2 {
		if (log_arr[0] == "4") && (len(log_arr) >= 15) {
			click_process(&log_arr)
		} else if (log_arr[0] == "3") && (len(log_arr) >= 15) {
			view_process(&log_arr)

		} else if (log_arr[0] == "5") && (len(log_arr) >= 15) {
			view_process(&log_arr)
		}
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
	did := (*strarr)[2]
	bid := (*strarr)[3]
	pid := (*strarr)[4]
	cid := (*strarr)[5]
	str_today := (*strarr)[1]
	if str_today == "1970-01-01" {
	} else {
		shift_g_recored_lock.RLock()

		//基础报表-------------------
		tb_base_key := "BASE^" + str_today + "^" + did + "^" + bid + "^" + pid + "^" + cid
		viewSetRecord(tb_base_key, did, bid, pid, cid)
		//blog(" BASE view str_today " + str_today + " did " + did + " bid " + bid + " pid " + pid + " cid " + cid)

		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}

}

func clickSetRecord(strkey string, did string, pid string, bid string, cid string) {
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
	tmprec.click_num++
	tmprec.m_lock.Unlock()
}

func click_process(strarr *[]string) {
	did := (*strarr)[2]
	bid := (*strarr)[3]
	pid := (*strarr)[4]
	cid := (*strarr)[5]
	str_today := (*strarr)[1]
	if str_today == "1970-01-01" {

	} else {
		shift_g_recored_lock.RLock()
		tb_base_key := "BASE^" + str_today + "^" + did + "^" + bid + "^" + pid + "^" + cid
		clickSetRecord(tb_base_key, did, bid, pid, cid)
		shift_g_recored_lock.RUnlock()
	}
}

// 执行一次写入redis
func updateRecord() {

	record_timestamp := time.Now().Unix()
	for {
		// 30秒刷新数据
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
}

func updateMap2Redis(idx int, rec_time int64) {
	redis_conn := pools_redis[idx].Get()
	defer redis_conn.Close()
	for redis_key, rpinfo := range g_recored[idx].m_record {

		res, err := redis_conn.Do("EXPIRE", redis_key, 172800)
		if err != nil {
			fmt.Println("ErrHs: ", res, "EXPIRE", redis_key, 172800)
		}

		var tmp_num int64
		tmp_num = 0
		if rpinfo.view_num > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "view", rpinfo.view_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "view", rpinfo.view_num)
			}
		}
		if rpinfo.click_num > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "click", rpinfo.click_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "click", rpinfo.click_num)
			}
		} else {
		}
		// 发起一个http的请求
		requestHttp(redis_key)

	}
	g_recored[idx].m_record = make(map[string]*reportInfo)
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

// 传入参数 第一个为日志收集类型 (syslog/kafka)
func main() {
	fmt.Println(" start")
	go loadsavetask()
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())
	var quit chan int
	initRecoredSet()
	initTaskCh()
	//if log_resource == "kafka" {
	//parseconf()
	initConfig()
	initRedis()
	//initKafkaConfig()
	go fillTask_kafka()
	//} else {
	go fillTask_syslog()
	//}
	for i := 0; i < count; i++ {
		go processTask(i)
	}
	go updateRecord()

	<-quit
}
