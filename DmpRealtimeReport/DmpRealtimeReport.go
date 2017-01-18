package main

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	kafka "github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
)

type reportInfo struct {
	order_id       string
	camp_id        string
	advert_id      string
	algorithm_type string

	true_cost       int64
	sys_cost        int64
	adv_cost        int64
	click_cost      int64
	kclick_cost     int64
	view_num        int64
	click_num       int64
	arrive_num      int64
	user_click_num  int64
	user_arrive_num int64

	two_jump_num int64
	alive_time   int64
	alive_num    int64

	shopcost int64
	m_lock   sync.Mutex

	action_map      map[string]int64
	user_action_map map[string]int64
}

type MediaBuyInfo struct {
	buy_type  string
	buy_price int64
}

var media_buy_info_map map[string]MediaBuyInfo
var media_buy_info_map_lock sync.RWMutex

var blacklistip_map map[string]int
var blacklistip_map_lock sync.RWMutex
var is_backup bool
var log_resource string

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

var order_shop chan string
var task_ch []chan string
var count int
var arrive_log_cache chan []string

var pools_redis []*redis.Pool
var pools_redis_click []*redis.Pool
var pools_redis_sid []*redis.Pool
var pools_redis_dcate []*redis.Pool
var pools_redis_mediabuy []*redis.Pool
var pools_redis_stopnotic *redis.Pool
var pools_redis_edp *redis.Pool
var pools_redis_clickinfo *redis.Pool

var domain_cate_map []map[string]string
var idx_domain_cate_map int
var count_domain_cate_map int

func getMediaBuyInfo(oid string) (bool, string, int64) {
	var tmp_res MediaBuyInfo
	b_res := false
	media_buy_info_map_lock.RLock()
	tmp_res, b_res = media_buy_info_map[oid]
	media_buy_info_map_lock.RUnlock()
	if b_res == false {
		redis_conn_mediabuy := pools_redis_mediabuy[0].Get()
		defer redis_conn_mediabuy.Close()
		mbuy_info, err := redis.String(redis_conn_mediabuy.Do("GET", oid))
		if err != nil {
			fmt.Println("getMediaBuyInfo Err:", mbuy_info, "GET", oid)
		}
		if len(mbuy_info) > 0 {
			var strarr []string = strings.Split(mbuy_info, "|")
			if len(strarr) == 2 {
				b_res = true
				tmp_res.buy_type = strarr[0]
				tmp_price, _ := strconv.Atoi(strarr[1])
				tmp_res.buy_price = int64(tmp_price)
				media_buy_info_map_lock.Lock()
				media_buy_info_map[oid] = tmp_res
				media_buy_info_map_lock.Unlock()
			}
		}
	}
	return b_res, tmp_res.buy_type, tmp_res.buy_price
}

func clearMediaBuyInfo() {
	media_buy_info_map_lock.Lock()
	media_buy_info_map = make(map[string]MediaBuyInfo)
	media_buy_info_map_lock.Unlock()
}

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

func requestHttp(str_task_key string) bool {
	hv := getCrc(str_task_key)
	str_hv := strconv.Itoa(int(hv))
	str_b64 := base64.StdEncoding.EncodeToString([]byte(str_task_key))
	str_url := "http://mysql.behe.com:8041/set?hv=" + str_hv + "&tsk=" + str_b64
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

func initDomainCateMap() {
	domain_cate_map = append(domain_cate_map, make(map[string]string))
	domain_cate_map = append(domain_cate_map, make(map[string]string))
	idx_domain_cate_map = 0
	count_domain_cate_map = 2
	loadDoaminCateMap(idx_domain_cate_map)
	fmt.Println("LoadDomainMap size:", len(domain_cate_map[idx_domain_cate_map]))
}

func loadDoaminCateMap(idx int) {
	redis_conn_dcate := pools_redis_dcate[0].Get()
	defer redis_conn_dcate.Close()
	res, err := redis.Values(redis_conn_dcate.Do("LRANGE", "domain_category", 0, -1))
	if err != nil {
		fmt.Println("loadDoaminCateMap Err:", res, "LRANGE", "domain_category", 0, -1)
		return
	}

	domain_cate_map[idx] = make(map[string]string)
	for i, data := range res {
		var strarr []string = strings.Split(string(data.([]byte)), ",")
		if len(strarr) == 3 {
			domain_cate_map[idx][strarr[0]] = strarr[1] + "^" + strarr[2]
		} else {
			fmt.Println("err len:", i)
		}
	}

}

func reloadDomainCateMap() {
	for {
		time.Sleep(600 * time.Second)
		loadDoaminCateMap((idx_domain_cate_map + 1) % count_domain_cate_map)
		idx_domain_cate_map = (idx_domain_cate_map + 1) % count_domain_cate_map
		clearMediaBuyInfo()
	}
}

func getDomainCate(str_domain string) string {
	res := domain_cate_map[idx_domain_cate_map][str_domain]
	if len(res) <= 0 {
		res = "0^0"
	}
	return res
}

/*
//------for arrive and action----------
var g_click_map []map[string]([]ClickInfo)
var click_map_idx int
var click_map_num int
var shift_g_click_map_lock sync.RWMutex
//-------------------------------------
func initClickMap(){
	g_click_map = append(g_click_map,make(map[string]([]ClickInfo)))
}
*/

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

func initTaskCh() {
	count = 32
	for i := 0; i < count; i++ {
		task_ch = append(task_ch, make(chan string, 300000))

	}
	arrive_log_cache = make(chan []string, 1000000)
	order_shop = make(chan string, 300000)
}

func fillTask_syslog() {
	//cmd := exec.Command("cat", "count.log.2015-09-23-10")
	cmd := exec.Command("cat", "dsp_pipe_count")
	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	inputReader := bufio.NewReader(stdout)
	for {
		inputString, readerError := inputReader.ReadString('\n')
		if readerError == io.EOF {
			time.Sleep(1 * time.Second)
			stdout.Close()
			cmd = exec.Command("cat", "dsp_pipe_count")
			stdout, _ = cmd.StdoutPipe()
			cmd.Start()
			inputReader = bufio.NewReader(stdout)
			continue
		}
		task_ch[getCrc(inputString)%uint32(count)] <- inputString
	}
}

func fillTask_kafka() {
	consumer, err := kafka.NewConsumer([]string{"kafka-0001:9092", "kafka-0002:9092", "kafka-0003:9092", "kafka-0004:9092", "kafka-0005:9092", "kafka-0006:9092", "kafka-0007:9092", "kafka-0008:9092"}, nil)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("adrtlog", 0, kafka.OffsetNewest)
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

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			//fmt.Println("Consumed message offset ", msg.Offset, string(msg.Value))
			task_ch[getCrc(string(msg.Value))%uint32(count)] <- string(msg.Value)
		case <-signals:
			break ConsumerLoop
		}
	}
}

func processTask(idx int) {
	for {
		str_log, ok := <-task_ch[idx]
		if ok == false {
			fmt.Println("task queue empty!")
		}
		Excute(str_log, idx)
	}
}

func Excute(str_log string, idx int) {
	str_log = strings.Replace(str_log, "\n", "", 1)
	str_log = strings.Replace(str_log, "\r", "", 1)
	str_log = strings.Replace(str_log, "\t", "", 1)
	var log_arr []string = strings.Split(str_log, sep_str)
	if len(log_arr) > 2 {
		if (log_arr[1] == "4") && (len(log_arr) >= 15) {
			click_process(&log_arr)
		}
		/*
				getad_process(&log_arr)
			} else if (log_arr[1] == "3") && (len(log_arr) >= 50) {
				view_process(&log_arr)
			} else if (log_arr[1] == "4") && (len(log_arr) >= 47) {
			} else if (log_arr[1] == "5") && (len(log_arr) >= 46) {
				winnotic_process(&log_arr)
			} else if (log_arr[1] == "6") && (len(log_arr) >= 9) {
				arrive_log_cache <- log_arr
				//arrive_process(&log_arr)
			}*/
	}
}

func thread_arrive_process() {
	time.Sleep(10 * time.Second)
	for {
		if len(arrive_log_cache) > 500 {
			time.Sleep(2 * time.Millisecond)
			tmp_log_arr := <-arrive_log_cache
			arrive_process(&tmp_log_arr)
		} else {
			time.Sleep(2 * time.Millisecond)
		}
	}

}

func getDomain(strurl string) string {
	res := strurl
	if len(strurl) <= 7 {
		return res
	}
	ipos := strings.Index(strurl, "http://")
	var str_para []byte
	if ipos >= 0 {
		str_para = []byte(strurl)[ipos+7:]
	}
	xpos := strings.Index(string(str_para), "/")
	var str_para_x []byte
	if xpos >= 0 {
		str_para_x = str_para[0:xpos]
		res = string(str_para_x)
	} else {
		res = string(str_para)
	}
	return res
}

func getadSetRecord(strkey string, oid string, cid string, advid string, algorithm_type string, realcost int64, agentcost int64, advcost int64) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	tmprec.true_cost += realcost
	if (algorithm_type != "2") && (algorithm_type != "3") {
		tmprec.sys_cost += agentcost
		tmprec.adv_cost += advcost
	}
	tmprec.m_lock.Unlock()
}

func getad_process(strarr *[]string) {
	adid := (*strarr)[3]
	oid := (*strarr)[4]
	/*ioid, _ := strconv.Atoi(oid)
	if ioid < 200000 {
		return
	}*/
	cid := (*strarr)[40]
	advid := (*strarr)[41]
	browser := (*strarr)[14]
	os := (*strarr)[15]
	if len(browser) <= 0 {
		browser = "0"
	}
	if len(os) <= 0 {
		os = "0"
	}
	slotid := (*strarr)[20]
	if len(slotid) <= 0 {
		slotid = "-1"
	}
	areaid := (*strarr)[13]
	domain := getDomain((*strarr)[19])
	if len(domain) <= 0 {
		domain = "-1"
	}
	esd := getMd5(slotid + "_" + domain)
	exchangeid := (*strarr)[9]
	//cs := (*strarr)[25]
	//channel := (*strarr)[24]
	mb_platform := (*strarr)[27]
	mb_brand := (*strarr)[28]
	mb_model := (*strarr)[29]
	mb_ntt := (*strarr)[30]
	mb_operater := (*strarr)[31]
	mb_appid := (*strarr)[37]

	media_type := (*strarr)[7]
	domain_category := getDomainCate(domain)
	if len(domain_category) <= 0 {
		domain_category = "0^0"
	}
	timestamp, _ := strconv.Atoi((*strarr)[2])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())
	var user_cate_list []string = strings.Split((*strarr)[39], ",")
	bid_time, _ := strconv.Atoi((*strarr)[24])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	user_age := "bh_ag_10000"
	if len((*strarr)[39]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		} else if strings.Contains(user_cate, "ag_") {
			user_age = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate = "bh_0"
		user_gender = "bh_gd_10000"
		user_age = "bh_ag_10000"
	}

	user_gender = strings.Replace(user_gender, "_LL", "", -1)
	user_cate = strings.Replace(user_cate, "_LL", "", -1)
	user_age = strings.Replace(user_age, "_LL", "", -1)

	advcost, _ := strconv.Atoi((*strarr)[18])
	agentcost, _ := strconv.Atoi((*strarr)[17])
	realcost, _ := strconv.Atoi((*strarr)[12])
	algorithm_type := (*strarr)[42]
	policy_id := (*strarr)[44]
	budgetId, _ := strconv.Atoi((*strarr)[36])
	// 增加 budgetid  小时 维度的报表，指标项跟其他项目一致

	adtype := 0
	if len((*strarr)) >= 46 {
		adtype, _ = strconv.Atoi((*strarr)[45])
	}

	if realcost > 0 && realcost < 1000000000 {

		shift_g_recored_lock.RLock()

		if adtype >= 2 {
			//移动APP报表-------------------
			mb_app_key := "APP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
			getadSetRecord(mb_app_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动平台报表-------------------
			mb_platform_key := "PLATF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_platform
			getadSetRecord(mb_platform_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端运营商报表-------------------
			mb_operater_key := "MOPER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_operater
			getadSetRecord(mb_operater_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端上网方式报表-------------------
			mb_ntt_key := "NTT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_ntt
			getadSetRecord(mb_ntt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端设备报表-------------------
			mb_device_key := "DEV^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_brand + "^" + mb_model
			getadSetRecord(mb_device_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//广告主实时30分钟报表-------------------
		tf_adv_m_key := "ADRM^" + str_today + "^" + advid + "^" + str_hour + "^" + str_minute
		getadSetRecord(tf_adv_m_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时 策略-------------------
		tf_h_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + policy_id
		getadSetRecord(tf_h_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日 策略---------------------
		tf_d_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + policy_id
		getadSetRecord(tf_d_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时-------------------
		tf_h_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour
		getadSetRecord(tf_h_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日---------------------
		tf_d_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid
		getadSetRecord(tf_d_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//浏览器报表-----------------------
		tf_br_key := "BROWER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + browser
		getadSetRecord(tf_br_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//操作系统报表-----------------------
		tf_os_key := "OPERA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + os
		getadSetRecord(tf_os_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//广告位报表-----------------------
		tf_esd_key := "MEDIAADID^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + esd + "^" + slotid + "^" + domain
		getadSetRecord(tf_esd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATD-----------------------
		atd_domain_key := "ATD^" + str_today + "^" + advid + "^" + domain
		getadSetRecord(atd_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//地域报表-----------------------
		tf_area_key := "PLACE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + areaid
		getadSetRecord(tf_area_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名报表-----------------------
		tf_domain_key := "MEDIA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain
		getadSetRecord(tf_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATS-----------------------
		ats_domain_key := "ATS^" + str_today + "^" + advid + "^" + esd + "^" + slotid + "^" + domain
		getadSetRecord(ats_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ExchangeID报表-----------------------
		tf_exid_key := "EXANGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + exchangeid
		getadSetRecord(tf_exid_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//频道报表-----------------------
		/*tf_ch_key:="CHANNEL^"+str_today+"^"+advid+"^"+cid+"^"+oid+"^"+channel+"^"+cs+"^"+exchangeid
		getadSetRecord(tf_ch_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))*/
		//媒体分类报表-----------------------
		tf_mt_key := "CONTENT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + media_type + "^" + exchangeid
		getadSetRecord(tf_mt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表-----------------------
		tf_material_key := "MATERIAL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid
		getadSetRecord(tf_material_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表 小时-----------------------
		tf_materialh_key := "MATERIALH^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid + "^" + str_hour
		getadSetRecord(tf_materialh_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名分类-------------------
		tf_dc_key := "DOMAINCATE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain_category
		getadSetRecord(tf_dc_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key := "CROWD^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_cate
			getadSetRecord(tf_crowd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key := "GENDER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_gender
			getadSetRecord(tf_gender_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//年龄报表-------------------
		if len(user_age) > 0 {
			tf_age_key := "AGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_age
			getadSetRecord(tf_age_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		// BudgetID报表------------------
		bg_hour_key := "BUDGET^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + budgetId
		getadSetRecord(bg_hour_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func viewSetRecord(strkey string, oid string, cid string, advid string, algorithm_type string, realcost int64, agentcost int64, advcost int64) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	tmprec.true_cost += realcost
	if (algorithm_type != "2") && (algorithm_type != "3") {
		tmprec.sys_cost += agentcost
		tmprec.adv_cost += advcost
	}
	tmprec.view_num++
	tmprec.m_lock.Unlock()
}

func view_process(strarr *[]string) {
	adid := (*strarr)[3]
	oid := (*strarr)[4]
	/*ioid, _ := strconv.Atoi(oid)
	if ioid < 200000 {
		return
	} */
	cid := (*strarr)[44]
	advid := (*strarr)[45]
	browser := (*strarr)[12]
	os := (*strarr)[13]
	if len(browser) <= 0 {
		browser = "0"
	}
	if len(os) <= 0 {
		os = "0"
	}
	slotid := (*strarr)[16]
	if len(slotid) <= 0 {
		slotid = "-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[15])
	if len(domain) <= 0 {
		domain = "-1"
	}
	esd := getMd5(slotid + "_" + domain)
	exchangeid := (*strarr)[5]
	cs := (*strarr)[25]
	channel := (*strarr)[24]
	media_type := (*strarr)[8]
	domain_category := getDomainCate(domain)
	if len(domain_category) <= 0 {
		domain_category = "0^0"
	}
	timestamp, _ := strconv.Atoi((*strarr)[2])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())
	var user_cate_list []string = strings.Split((*strarr)[43], ",")
	bid_time, _ := strconv.Atoi((*strarr)[23])
	if isSameDay(timestamp, bid_time) == false {
		return
	}
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	user_age := "bh_ag_10000"
	if len((*strarr)[43]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		} else if strings.Contains(user_cate, "ag_") {
			user_age = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate = "bh_0"
		user_gender = "bh_gd_10000"
		user_age = "bh_ag_10000"
	}

	user_gender = strings.Replace(user_gender, "_LL", "", -1)
	user_cate = strings.Replace(user_cate, "_LL", "", -1)
	user_age = strings.Replace(user_age, "_LL", "", -1)

	advcost, _ := strconv.Atoi((*strarr)[20])
	agentcost, _ := strconv.Atoi((*strarr)[19])
	realcost, _ := strconv.Atoi((*strarr)[18])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[46])
	algorithm_type := (*strarr)[46]
	policy_id := (*strarr)[51]
	//----------mobile---------------
	mb_platform := (*strarr)[29]
	mb_brand := (*strarr)[30]
	mb_model := (*strarr)[31]
	mb_ntt := (*strarr)[32]
	mb_operater := (*strarr)[33]
	mb_appid := (*strarr)[40]
	budgetId, _ := strconv.Atoi((*strarr)[39])

	adtype := 0
	if len((*strarr)) >= 56 {
		adtype, _ = strconv.Atoi((*strarr)[55])
	}
	//-----media_buy----------------
	is_umod := (*strarr)[42]
	if is_umod == "1" {
		umod_ok, mb_type, mb_price := getMediaBuyInfo(oid)
		if umod_ok && mb_type == "cpm" {
			advcost = int(mb_price) / 1000
		}
	}
	//------------------------------
	if realcost < 1000000000 {

		shift_g_recored_lock.RLock()

		if adtype >= 2 {
			//移动APP报表-------------------
			mb_app_key := "APP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
			viewSetRecord(mb_app_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动平台报表-------------------
			mb_platform_key := "PLATF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_platform
			viewSetRecord(mb_platform_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端运营商报表-------------------
			mb_operater_key := "MOPER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_operater
			viewSetRecord(mb_operater_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端上网方式报表-------------------
			mb_ntt_key := "NTT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_ntt
			viewSetRecord(mb_ntt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端设备报表-------------------
			mb_device_key := "DEV^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_brand + "^" + mb_model
			viewSetRecord(mb_device_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//广告主实时30分钟报表-------------------
		tf_adv_m_key := "ADRM^" + str_today + "^" + advid + "^" + str_hour + "^" + str_minute
		viewSetRecord(tf_adv_m_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATS-----------------------
		ats_domain_key := "ATS^" + str_today + "^" + advid + "^" + esd + "^" + slotid + "^" + domain
		viewSetRecord(ats_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATD-----------------------
		atd_domain_key := "ATD^" + str_today + "^" + advid + "^" + domain
		viewSetRecord(atd_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时 策略-------------------
		tf_h_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + policy_id
		viewSetRecord(tf_h_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日 策略---------------------
		tf_d_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + policy_id
		viewSetRecord(tf_d_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时-------------------
		tf_h_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour
		viewSetRecord(tf_h_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日---------------------
		tf_d_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid
		viewSetRecord(tf_d_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//浏览器报表-----------------------
		tf_br_key := "BROWER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + browser
		viewSetRecord(tf_br_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//操作系统报表-----------------------
		tf_os_key := "OPERA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + os
		viewSetRecord(tf_os_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//广告位报表-----------------------
		tf_esd_key := "MEDIAADID^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + esd + "^" + slotid + "^" + domain
		viewSetRecord(tf_esd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//地域报表-----------------------
		tf_area_key := "PLACE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + areaid
		viewSetRecord(tf_area_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名报表-----------------------
		tf_domain_key := "MEDIA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain
		viewSetRecord(tf_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ExchangeID报表-----------------------
		tf_exid_key := "EXANGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + exchangeid
		viewSetRecord(tf_exid_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//频道报表-----------------------
		if len(channel) > 0 || len(cs) > 0 {
			if len(channel) == 0 {
				channel = "-1"
			}
			if len(cs) == 0 {
				cs = "-1"
			}
			tf_ch_key := "CHANNEL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + channel + "^" + cs + "^" + exchangeid
			viewSetRecord(tf_ch_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//媒体分类报表-----------------------
		tf_mt_key := "CONTENT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + media_type + "^" + exchangeid
		viewSetRecord(tf_mt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表-----------------------
		tf_material_key := "MATERIAL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid
		viewSetRecord(tf_material_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表 小时-----------------------
		tf_materialh_key := "MATERIALH^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid + "^" + str_hour
		viewSetRecord(tf_materialh_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名分类-------------------
		tf_dc_key := "DOMAINCATE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain_category
		viewSetRecord(tf_dc_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key := "CROWD^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_cate
			viewSetRecord(tf_crowd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key := "GENDER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_gender
			viewSetRecord(tf_gender_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}

		//年龄报表-------------------
		if len(user_age) > 0 {
			tf_age_key := "AGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_age
			viewSetRecord(tf_age_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		// BudgetID报表------------------
		bg_hour_key := "BUDGET^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + budgetId
		viewSetRecord(bg_hour_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))

		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func winnoticSetRecord(strkey string, oid string, cid string, advid string, algorithm_type string, realcost int64, agentcost int64, advcost int64) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	tmprec.true_cost += int64(realcost)
	if (algorithm_type != "2") && (algorithm_type != "3") {
		tmprec.sys_cost += int64(agentcost)
		tmprec.adv_cost += int64(advcost)
	}
	tmprec.m_lock.Unlock()
}

func winnotic_process(strarr *[]string) {
	adid := (*strarr)[3]
	oid := (*strarr)[4]
	/*ioid, _ := strconv.Atoi(oid)
	if ioid < 200000 {
		return
	}*/
	cid := (*strarr)[43]
	advid := (*strarr)[44]
	browser := (*strarr)[12]
	os := (*strarr)[13]
	if len(browser) <= 0 {
		browser = "0"
	}
	if len(os) <= 0 {
		os = "0"
	}
	slotid := (*strarr)[16]
	if len(slotid) <= 0 {
		slotid = "-1"
	}
	areaid := (*strarr)[11]
	domain := getDomain((*strarr)[15])
	if len(domain) <= 0 {
		domain = "-1"
	}
	esd := getMd5(slotid + "_" + domain)
	exchangeid := (*strarr)[5]
	cs := (*strarr)[25]
	channel := (*strarr)[24]
	media_type := (*strarr)[8]
	domain_category := getDomainCate(domain)
	if len(domain_category) <= 0 {
		domain_category = "0^0"
	}
	timestamp, _ := strconv.Atoi((*strarr)[2])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())
	var user_cate_list []string = strings.Split((*strarr)[42], ",")
	bid_time, _ := strconv.Atoi((*strarr)[23])
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	user_age := "bh_ag_10000"
	if len((*strarr)[42]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		} else if strings.Contains(user_cate, "ag_") {
			user_age = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate = "bh_0"
		user_gender = "bh_gd_10000"
		user_age = "bh_ag_10000"
	}

	user_gender = strings.Replace(user_gender, "_LL", "", -1)
	user_cate = strings.Replace(user_cate, "_LL", "", -1)
	user_age = strings.Replace(user_age, "_LL", "", -1)

	advcost, _ := strconv.Atoi((*strarr)[20])
	agentcost, _ := strconv.Atoi((*strarr)[19])
	realcost, _ := strconv.Atoi((*strarr)[18])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[46])
	algorithm_type := (*strarr)[45]
	policy_id := (*strarr)[47]

	//--------mobile---------------
	mb_platform := (*strarr)[29]
	mb_brand := (*strarr)[30]
	mb_model := (*strarr)[31]
	mb_ntt := (*strarr)[32]
	mb_operater := (*strarr)[33]
	mb_appid := (*strarr)[40]
	budgetId, _ := strconv.Atoi((*strarr)[39])

	adtype := 0
	if len((*strarr)) >= 49 {
		adtype, _ = strconv.Atoi((*strarr)[48])
	}
	//-----------------------------
	if realcost > 0 && realcost < 1000000000 {

		shift_g_recored_lock.RLock()

		if adtype >= 2 {
			//移动APP报表-------------------
			mb_app_key := "APP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
			winnoticSetRecord(mb_app_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动平台报表-------------------
			mb_platform_key := "PLATF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_platform
			winnoticSetRecord(mb_platform_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端运营商报表-------------------
			mb_operater_key := "MOPER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_operater
			winnoticSetRecord(mb_operater_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端上网方式报表-------------------
			mb_ntt_key := "NTT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_ntt
			winnoticSetRecord(mb_ntt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
			//移动端设备报表-------------------
			mb_device_key := "DEV^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_brand + "^" + mb_model
			winnoticSetRecord(mb_device_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//广告主实时30分钟报表-------------------
		tf_adv_m_key := "ADRM^" + str_today + "^" + advid + "^" + str_hour + "^" + str_minute
		winnoticSetRecord(tf_adv_m_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATS-----------------------
		ats_domain_key := "ATS^" + str_today + "^" + advid + "^" + esd + "^" + slotid + "^" + domain
		winnoticSetRecord(ats_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ATD-----------------------
		atd_domain_key := "ATD^" + str_today + "^" + advid + "^" + domain
		winnoticSetRecord(atd_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时 策略-------------------
		tf_h_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + policy_id
		winnoticSetRecord(tf_h_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日 策略---------------------
		tf_d_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + policy_id
		winnoticSetRecord(tf_d_p_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 小时-------------------
		tf_h_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour
		winnoticSetRecord(tf_h_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//时间报表 日---------------------
		tf_d_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid
		winnoticSetRecord(tf_d_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//浏览器报表-----------------------
		tf_br_key := "BROWER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + browser
		winnoticSetRecord(tf_br_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//操作系统报表-----------------------
		tf_os_key := "OPERA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + os
		winnoticSetRecord(tf_os_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//广告位报表-----------------------
		tf_esd_key := "MEDIAADID^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + esd + "^" + slotid + "^" + domain
		winnoticSetRecord(tf_esd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//地域报表-----------------------
		tf_area_key := "PLACE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + areaid
		winnoticSetRecord(tf_area_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名报表-----------------------
		tf_domain_key := "MEDIA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain
		winnoticSetRecord(tf_domain_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//ExchangeID报表-----------------------
		tf_exid_key := "EXANGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + exchangeid
		winnoticSetRecord(tf_exid_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//频道报表-----------------------
		if len(channel) > 0 || len(cs) > 0 {
			if len(channel) == 0 {
				channel = "-1"
			}
			if len(cs) == 0 {
				cs = "-1"
			}
			tf_ch_key := "CHANNEL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + channel + "^" + cs + "^" + exchangeid
			winnoticSetRecord(tf_ch_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//媒体分类报表-----------------------
		tf_mt_key := "CONTENT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + media_type + "^" + exchangeid
		winnoticSetRecord(tf_mt_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表-----------------------
		tf_material_key := "MATERIAL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid
		winnoticSetRecord(tf_material_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//素材报表 小时-----------------------
		tf_materialh_key := "MATERIALH^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid + "^" + str_hour
		winnoticSetRecord(tf_materialh_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//域名分类-------------------
		tf_dc_key := "DOMAINCATE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain_category
		winnoticSetRecord(tf_dc_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key := "CROWD^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_cate
			winnoticSetRecord(tf_crowd_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key := "GENDER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_gender
			winnoticSetRecord(tf_gender_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}

		//年龄报表-------------------
		if len(user_age) > 0 {
			tf_age_key := "AGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_age
			winnoticSetRecord(tf_age_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))
		}
		// BudgetID报表------------------
		bg_hour_key := "BUDGET^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + budgetId
		winnoticSetRecord(bg_hour_key, oid, cid, advid, algorithm_type, int64(realcost), int64(agentcost), int64(advcost))

		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func record_Click(bhuid string, order_id string, clickinfo string) int {
	if len(bhuid) <= 0 {
		return 0
	}
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

func isCheatClick(ip string, uniqcode string, bidtime int64, click_time int64, oid string, is_umod string) bool {
	blacklistip_map_lock.RLock()
	if blacklistip_map[ip] == 1 {
		blacklistip_map_lock.RUnlock()
		return true
	}
	blacklistip_map_lock.RUnlock()

	if is_umod == "1" {
		return false
	}
	if (click_time - bidtime) > 2*86400 {
		return true
	}

	redis_conn := pools_redis_clickinfo.Get()
	defer redis_conn.Close()
	var res int
	var err error
	if is_backup == false {
		res, err = redis.Int(redis_conn.Do("HINCRBY", "u_"+uniqcode, "reporter", 1))
	} else {
		res, err = redis.Int(redis_conn.Do("HGET", "u_"+uniqcode, "reporter"))
	}
	if err != nil {
		fmt.Println("ErrHi:", res, "HINCRBY", uniqcode, "reporter", 1)
		return false
	}
	if res == 1 {
		redis_conn.Do("EXPIRE", "u_"+uniqcode, 2*86400)
	}
	if res > 3 {
		return true
	}

	return false
}

func recoredClick2AfterReport(unicode_id string, clickinfo string) {
	if len(unicode_id) <= 0 {
		return
	}
	if len(clickinfo) <= 0 {
		return
	}
	redis_conn := pools_redis_clickinfo.Get()
	defer redis_conn.Close()
	res, err := redis_conn.Do("SETEX", unicode_id, 432000, clickinfo)
	if err != nil {
		fmt.Println("ErrHs:", res, "SETEX", unicode_id, 432000, clickinfo)
	}
	return
}

func clickSetRecord(strkey string, oid string, cid string, advid string, algorithm_type string, clickcost int64, kclickcost int64, advcost int64, isuser_click int, is_umod string, tmp_is_order_stop bool) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	tmprec.click_cost += int64(clickcost)
	if tmp_is_order_stop == false {
		tmprec.kclick_cost += int64(kclickcost)
	}
	if is_umod == "1" {
		tmprec.kclick_cost += int64(kclickcost)
		tmprec.adv_cost += int64(advcost)
	}
	tmprec.click_num++
	if isuser_click == 1 {
		tmprec.user_click_num++
	}
	tmprec.m_lock.Unlock()
}

func click_process(strarr *[]string) {
	cid, _ = strconv.Atoi((*strarr)[5])
	timestamp, _ := strconv.Atoi((*strarr)[1])
	str_today := time.Unix(int64(timestamp), 0).Format("2006-01-02")
	str_hour := strconv.Itoa(time.Unix(int64(timestamp), 0).Hour())
	str_minute := strconv.Itoa(time.Unix(int64(timestamp), 0).Minute())

	br_app_key := "BR^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
	clickSetRecord(mb_app_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)

	var user_cate_list []string = strings.Split((*strarr)[41], ",")
	bid_time, _ := strconv.Atoi((*strarr)[19])
	is_umod := (*strarr)[37]
	uniqcode := (*strarr)[9]
	ip := (*strarr)[10]
	if isCheatClick(ip, uniqcode, int64(bid_time), int64(timestamp), oid, is_umod) {
		return
	}
	user_cate := "bh_0"
	user_gender := "bh_gd_10000"
	user_age := "bh_ag_10000"
	if len((*strarr)[41]) > 2 {
		user_cate = user_cate_list[bid_time%len(user_cate_list)]
		if strings.Contains(user_cate, "gd_") {
			user_gender = user_cate
			user_cate = "bh_0"
		} else if strings.Contains(user_cate, "ag_") {
			user_age = user_cate
			user_cate = "bh_0"
		}
	}
	if len(user_cate) <= 0 {
		user_cate = "bh_0"
		user_gender = "bh_gd_10000"
		user_age = "bh_ag_10000"
	}

	user_gender = strings.Replace(user_gender, "_LL", "", -1)
	user_cate = strings.Replace(user_cate, "_LL", "", -1)
	user_age = strings.Replace(user_age, "_LL", "", -1)

	bhuid := (*strarr)[7]
	unicode_id := (*strarr)[9]
	user_ip := (*strarr)[10]
	clickcost, _ := strconv.Atoi((*strarr)[45])
	kclickcost, _ := strconv.Atoi((*strarr)[46])
	//algorithm_type ,_ :=strconv.Atoi((*strarr)[44])
	algorithm_type := (*strarr)[44]
	domain_category := getDomainCate(domain)
	if len(domain_category) <= 0 {
		domain_category = "0^0"
	}
	policy_id := (*strarr)[48]
	//-------------mobile--------------------
	mb_platform := (*strarr)[25]
	mb_brand := (*strarr)[26]
	mb_model := (*strarr)[27]
	mb_ntt := (*strarr)[28]
	mb_operater := (*strarr)[29]
	mb_appid := (*strarr)[38]
	budgetId, _ := strconv.Atoi((*strarr)[36])
	adtype := 0
	str_adtype := "0"
	if len((*strarr)) >= 51 {
		adtype, _ = strconv.Atoi((*strarr)[50])
		str_adtype = (*strarr)[50]
	}
	mrec := mb_platform + "&" + mb_brand + "&" + mb_model + "&" + mb_ntt + "&" + mb_operater + "&" + mb_appid + "&" + str_adtype
	//---------------------------------------

	click_info := adid + "|" + oid + "|" + cid + "|" + advid + "|" + browser + "|" + os + "|" + slotid + "|" + areaid + "|" + domain + "|" + esd + "|" + exchangeid + "|" + cs + "|" + channel + "|" + media_type + "|" + str_today + "|" + str_hour + "|" + click_time + "|" + algorithm_type + ":" + user_cate + ":" + policy_id + ":" + is_umod + ":" + mrec
	isuser_click := record_Click(bhuid, oid, click_info)
	//-----click info recored to after report-----------------
	tmp_map := make(map[string]string)
	tmp_map["domain"] = domain
	tmp_map["place_id"] = slotid
	tmp_map["behe_userid"] = bhuid
	tmp_map["exchange_id"] = exchangeid
	tmp_map["user_ip"] = user_ip
	tmp_map["order_id"] = oid
	tmp_map["ad_id"] = adid
	tmp_map["campaign_id"] = cid
	tmp_map["account_id"] = advid
	tmp_map["time_info"] = strconv.Itoa(timestamp)
	tmp_map_obj_str, _ := json.Marshal(tmp_map)
	if is_backup == false {
		recoredClick2AfterReport(unicode_id, string(tmp_map_obj_str))
	}
	//-----media_buy----------------

	advcost := 0

	if is_umod == "1" {
		umod_ok, mb_type, mb_price := getMediaBuyInfo(oid)
		if umod_ok && mb_type == "cpc" {
			advcost = int(mb_price)
			kclickcost = int(mb_price)
		}
	}
	//------------------------------
	tmp_is_order_stop := isOrderStop(oid)
	//------------------------------
	{

		shift_g_recored_lock.RLock()
		if adtype >= 2 {
			//移动APP报表-------------------
			mb_app_key := "APP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
			clickSetRecord(mb_app_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
			//移动平台报表-------------------
			mb_platform_key := "PLATF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_platform
			clickSetRecord(mb_platform_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
			//移动端运营商报表-------------------
			mb_operater_key := "MOPER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_operater
			clickSetRecord(mb_operater_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
			//移动端上网方式报表-------------------
			mb_ntt_key := "NTT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_ntt
			clickSetRecord(mb_ntt_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
			//移动端设备报表-------------------
			mb_device_key := "DEV^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_brand + "^" + mb_model
			clickSetRecord(mb_device_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		}
		//广告主实时30分钟报表-------------------
		tf_adv_m_key := "ADRM^" + str_today + "^" + advid + "^" + str_hour + "^" + str_minute
		clickSetRecord(tf_adv_m_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//ATS-----------------------
		ats_domain_key := "ATS^" + str_today + "^" + advid + "^" + esd + "^" + slotid + "^" + domain
		clickSetRecord(ats_domain_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//ATD-----------------------
		atd_domain_key := "ATD^" + str_today + "^" + advid + "^" + domain
		clickSetRecord(atd_domain_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//时间报表 小时 策略-------------------
		tf_h_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + policy_id
		clickSetRecord(tf_h_p_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//时间报表 日 策略---------------------
		tf_d_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + policy_id
		clickSetRecord(tf_d_p_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//时间报表 小时-------------------
		tf_h_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour
		clickSetRecord(tf_h_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//时间报表 日---------------------
		tf_d_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid
		clickSetRecord(tf_d_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//浏览器报表-----------------------
		tf_br_key := "BROWER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + browser
		clickSetRecord(tf_br_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//操作系统报表-----------------------
		tf_os_key := "OPERA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + os
		clickSetRecord(tf_os_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//广告位报表-----------------------
		tf_esd_key := "MEDIAADID^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + esd + "^" + slotid + "^" + domain
		clickSetRecord(tf_esd_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//地域报表-----------------------
		tf_area_key := "PLACE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + areaid
		clickSetRecord(tf_area_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//域名报表-----------------------
		tf_domain_key := "MEDIA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain
		clickSetRecord(tf_domain_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//ExchangeID报表-----------------------
		tf_exid_key := "EXANGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + exchangeid
		clickSetRecord(tf_exid_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//频道报表-----------------------
		if len(channel) > 0 || len(cs) > 0 {
			if len(channel) == 0 {
				channel = "-1"
			}
			if len(cs) == 0 {
				cs = "-1"
			}
			tf_ch_key := "CHANNEL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + channel + "^" + cs + "^" + exchangeid
			clickSetRecord(tf_ch_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		}
		//媒体分类报表-----------------------
		tf_mt_key := "CONTENT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + media_type + "^" + exchangeid
		clickSetRecord(tf_mt_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//素材报表-----------------------
		tf_material_key := "MATERIAL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid
		clickSetRecord(tf_material_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//素材报表 小时-----------------------
		tf_materialh_key := "MATERIALH^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid + "^" + str_hour
		clickSetRecord(tf_materialh_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//域名分类-------------------
		tf_dc_key := "DOMAINCATE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain_category
		clickSetRecord(tf_dc_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key := "CROWD^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_cate
			clickSetRecord(tf_crowd_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key := "GENDER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_gender
			clickSetRecord(tf_gender_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		}
		//年龄报表-------------------
		if len(user_age) > 0 {
			tf_age_key := "AGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_age
			clickSetRecord(tf_age_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)
		}
		// BudgetID报表------------------
		bg_hour_key := "BUDGET^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + budgetId
		clickSetRecord(bg_hour_key, oid, cid, advid, algorithm_type, int64(clickcost), int64(kclickcost), int64(advcost), isuser_click, is_umod, tmp_is_order_stop)

		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func checkArrive(bhuid string, sid string, mbeahivor string) (bool, string, bool, bool) {
	if len(bhuid) <= 0 {
		return false, "", false, false
	}
	if len(sid) <= 0 {
		return false, "", false, false
	}
	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()
	click_rec, err := redis.StringMap(redis_conn_click.Do("HGETALL", bhuid))
	if err != nil {
		fmt.Println("checkArrive Err:", click_rec, "HGETALL", bhuid)
		return false, "", false, false
	}
	//fmt.Println("HGETALL",bhuid,",click_rec:",click_rec,",err:",err)
	if len(click_rec) <= 0 {
		return false, "", false, false
	}

	res := false
	res_str := ""
	click_time := int64(0)
	is_user_arrive := true
	is_user_action := false
	if strings.Contains(sid, ".") {
		is_user_action = true
	}

	redis_conn_sid := pools_redis_sid[0].Get()
	defer redis_conn_sid.Close()
	oidlist, err := redis.String(redis_conn_sid.Do("GET", sid))
	if err != nil {
		fmt.Println("checkArrive Err:", oidlist, "GET", sid)
		return res, res_str, is_user_arrive, is_user_action
	}
	//fmt.Println("SID:",sid,",olist:",oidlist)
	target_oid := "0"
	isnew := true
	str_rec_action := ""
	for oid, oidinfo := range click_rec {
		if strings.Contains(oidlist, oid) {
			var info_arr []string = strings.Split(oidinfo, "|")
			if len(info_arr) >= 18 {
				tmp_time, _ := strconv.Atoi(info_arr[16])
				if int64(tmp_time) > click_time {
					target_oid = oid
					click_time = int64(tmp_time)
					res_str = oidinfo
					res = true
					if len(info_arr) >= 19 {
						is_user_arrive = false
					}
					if len(info_arr) == 20 {
						str_rec_action = info_arr[19]
						isnew = false
						if strings.Contains(info_arr[19], ("(" + mbeahivor + ")")) {
							is_user_action = false
						}

					}
				}
			}
		}
	}
	if res == true && is_user_arrive == true {
		now_time := time.Now().Unix()
		today := (now_time+28800)/86400*86400 - 28800
		redis_conn_click.Do("HSET", bhuid, target_oid, res_str+"|1")
		redis_conn_click.Do("EXPIREAT", bhuid, today+86399)
	}
	if res == true && is_user_action == true {
		now_time := time.Now().Unix()
		today := (now_time+28800)/86400*86400 - 28800
		str_rec_action = str_rec_action + ("(" + mbeahivor + ")")
		if isnew {
			redis_conn_click.Do("HSET", bhuid, target_oid, res_str+"|"+str_rec_action)
		} else {
			pos_20 := strings.LastIndex(res_str, "|")
			tmp_byte := []byte(res_str)[0 : pos_20+1]
			//fmt.Println(string(tmp_byte))
			redis_conn_click.Do("HSET", bhuid, target_oid, string(tmp_byte)+str_rec_action)
		}
		redis_conn_click.Do("EXPIREAT", bhuid, today+86399)
	}
	return res, res_str, is_user_arrive, is_user_action
}

func calculateAliveTime(bhuid string, oid string, sessionid string, now_time int64) int64 {
	redis_conn_click := pools_redis_click[0].Get()
	defer redis_conn_click.Close()

	res := int64(0)
	last_session_time, err := redis.Int64(redis_conn_click.Do("HGET", "alive_"+bhuid, (oid + "_" + sessionid)))
	if err != nil {
		fmt.Println("ErrHs:", last_session_time, "HGET", "alive_"+bhuid, (oid + "_" + sessionid))
	}
	if last_session_time > 0 {
		res = now_time - last_session_time
		if res < 0 {
			res = 0
		}
	}
	redis_conn_click.Do("HSET", "alive_"+bhuid, (oid + "_" + sessionid), now_time)
	redis_conn_click.Do("EXPIRE", "alive_"+bhuid, 7200)
	return res
}

func raSetRecord(strkey string, oid string, cid string, advid string, algorithm_type string, is_umod bool, kclick_cost int64, advcost int64, isarrive bool, is_0_arrive bool, is_user_arrive bool, is_user_action bool, action_key string, shopcost int64) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	if isarrive == true {
		if is_0_arrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		}
	} else {
		tmprec.shopcost += shopcost
		tmprec.action_map[action_key]++
		if is_user_action == true {
			tmprec.user_action_map[action_key]++
		}
	}
	if is_umod {
		tmprec.kclick_cost += int64(kclick_cost)
		tmprec.adv_cost += int64(advcost)
	}
	tmprec.m_lock.Unlock()
}

func raSetRecord_time(strkey string, oid string, cid string, advid string, algorithm_type string, is_umod bool, kclick_cost int64, advcost int64, isarrive bool, is_0_arrive bool, is_user_arrive bool, is_user_action bool, action_key string, alive_time int64, is_two_jump bool) {
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
	tmprec.order_id = oid
	tmprec.camp_id = cid
	tmprec.advert_id = advid
	tmprec.algorithm_type = algorithm_type

	if isarrive == true {
		if is_0_arrive == true {
			tmprec.arrive_num++
			if is_user_arrive == true {
				tmprec.user_arrive_num++
			}
		} else {
			tmprec.alive_num++
			tmprec.alive_time += alive_time
		}
		if is_two_jump == true {
			tmprec.two_jump_num++
		}
	} else {
		tmprec.action_map[action_key]++
		if is_user_action == true {
			tmprec.user_action_map[action_key]++
		}
	}
	if is_umod {
		tmprec.kclick_cost += int64(kclick_cost)
		tmprec.adv_cost += int64(advcost)
	}
	tmprec.m_lock.Unlock()
}

func arrive_process(strarr *[]string) {
	bhuid := (*strarr)[5]
	uri := (*strarr)[6]
	timestamp, _ := strconv.Atoi((*strarr)[2])
	prara_map := make(map[string]string)
	paraURI(uri, &prara_map)
	sid := prara_map["si"]
	sessionid := prara_map["se"]
	mbeahivor := prara_map["at"]
	if len(mbeahivor) <= 0 {
		mbeahivor = "action"
	}
	strshopcost := prara_map["cost"]
	fshopcost, _ := strconv.ParseFloat(strshopcost, 64)
	shopcost := int64(fshopcost * 1000000)
	shoporderid := prara_map["orderid"]
	isUmod := "0"
	is_two_jump := false
	is_0_arrive := true
	isok, str_info, is_user_arrive, is_user_action := checkArrive(bhuid, sid, mbeahivor)
	isarrive := true
	if strings.Contains(sid, ".") {
		isarrive = false
	}

	//two_jump_num
	if isok == true {
		var info_arr []string = strings.Split(str_info, "|")
		if len(info_arr) < 18 {
			return
		}

		var session_id_arr []string = strings.Split(sessionid, "_")
		if len(session_id_arr) == 2 {
			i_sn, _ := strconv.Atoi(session_id_arr[1])
			if i_sn == 1 {
				is_two_jump = true
			}
			if i_sn > 0 {
				is_0_arrive = false
			}
		}
		//------------------
		adid := info_arr[0]
		oid := info_arr[1]
		cid := info_arr[2]
		advid := info_arr[3]
		browser := info_arr[4]
		os := info_arr[5]
		slotid := info_arr[6]
		areaid := info_arr[7]
		domain := info_arr[8]
		if len(domain) <= 0 {
			domain = "-1"
		}
		esd := info_arr[9]
		exchangeid := info_arr[10]
		cs := info_arr[11]
		channel := info_arr[12]
		media_type := info_arr[13]
		str_today := info_arr[14]
		str_hour := info_arr[15]
		algorithm_type := info_arr[17]
		user_cate := "bh_0"
		user_gender := "bh_gd_10000"
		user_age := "bh_ag_10000"
		alive_time := int64(0)
		var tmp_arr []string = strings.Split(info_arr[17], ":")
		var policy_id string
		mrec := ""
		if len(tmp_arr) >= 2 {
			algorithm_type = tmp_arr[0]
			user_cate = tmp_arr[1]
			if strings.Contains(user_cate, "gd_") {
				user_gender = user_cate
				user_cate = "bh_0"
			} else if strings.Contains(user_cate, "ag_") {
				user_age = user_cate
				user_cate = "bh_0"
			}

			if len(tmp_arr) >= 3 {
				policy_id = tmp_arr[2]
			}
			if len(tmp_arr) >= 4 {
				isUmod = tmp_arr[3]
			}
			if len(tmp_arr) >= 5 {
				mrec = tmp_arr[4]
			}
		}
		if len(user_cate) <= 0 {
			user_cate = "bh_0"
			user_gender = "bh_gd_10000"
			user_age = "bh_ag_10000"
		}

		user_gender = strings.Replace(user_gender, "_LL", "", -1)
		user_cate = strings.Replace(user_cate, "_LL", "", -1)
		user_age = strings.Replace(user_age, "_LL", "", -1)

		if isarrive == true && len(session_id_arr) == 2 {
			alive_time = calculateAliveTime(bhuid, oid, session_id_arr[0], int64(timestamp))
		}
		domain_category := getDomainCate(domain)
		if len(domain_category) <= 0 {
			domain_category = "0^0"
		}
		var is_umod bool = false
		advcost := 0
		kclick_cost := 0

		if (isarrive == false) && (isUmod == "1") {
			//-----media_buy----------------
			umod_ok, mb_type, mb_price := getMediaBuyInfo(oid)
			if umod_ok && mb_type == "cpa" {
				is_umod = umod_ok
				advcost = int(mb_price)
				kclick_cost = int(mb_price)
			}
			//------------------------------
		}
		//------------------

		//----------Mobile---------------
		var tmp_mrec_arr []string = strings.Split(mrec, "&")
		var mb_platform string
		var mb_brand string
		var mb_model string
		var mb_ntt string
		var mb_operater string
		var mb_appid string
		var str_adtype string
		var adtype int
		if len(tmp_mrec_arr) >= 7 {
			mb_platform = tmp_mrec_arr[0]
			mb_brand = tmp_mrec_arr[1]
			mb_model = tmp_mrec_arr[2]
			mb_ntt = tmp_mrec_arr[3]
			mb_operater = tmp_mrec_arr[4]
			mb_appid = tmp_mrec_arr[5]
			str_adtype = tmp_mrec_arr[6]
			adtype, _ = strconv.Atoi(str_adtype)
		}
		//-------------------------------

		shift_g_recored_lock.RLock()

		//电商订单时间表-------------------
		//广告主id,  报表日期,       活动id,      订单id,shop订单id,订单价格,日志时间
		if len(shoporderid) > 0 {
			order_shop <- (advid + "^" + str_today + "^" + cid + "^" + oid + "^" + shoporderid + "^" + strconv.Itoa(int(shopcost)) + "^" + strconv.Itoa(int(time.Now().Unix())))
		}

		if adtype >= 2 {
			//移动APP报表-------------------
			mb_app_key := "APP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_appid + "^" + slotid
			raSetRecord(mb_app_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
			//移动平台报表-------------------
			mb_platform_key := "PLATF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_platform
			raSetRecord(mb_platform_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
			//移动端运营商报表-------------------
			mb_operater_key := "MOPER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_operater
			raSetRecord(mb_operater_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
			//移动端上网方式报表-------------------
			mb_ntt_key := "NTT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_ntt
			raSetRecord(mb_ntt_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
			//移动端设备报表-------------------
			mb_device_key := "DEV^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + mb_brand + "^" + mb_model
			raSetRecord(mb_device_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		}
		//ATS-----------------------
		ats_domain_key := "ATS^" + str_today + "^" + advid + "^" + esd + "^" + slotid + "^" + domain
		raSetRecord(ats_domain_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//ATD-----------------------
		atd_domain_key := "ATD^" + str_today + "^" + advid + "^" + domain
		raSetRecord(atd_domain_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//时间报表 小时 策略-------------------
		tf_h_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour + "^" + policy_id
		raSetRecord(tf_h_p_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//时间报表 日 策略---------------------
		tf_d_p_key := "TFP^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + policy_id
		raSetRecord(tf_d_p_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//时间报表 小时-------------------
		tf_h_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + str_hour
		raSetRecord(tf_h_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, shopcost)
		//时间报表 日---------------------
		tf_d_key := "TF^" + str_today + "^" + advid + "^" + cid + "^" + oid
		raSetRecord(tf_d_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, shopcost)
		//浏览器报表-----------------------
		tf_br_key := "BROWER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + browser
		raSetRecord(tf_br_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//操作系统报表-----------------------
		tf_os_key := "OPERA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + os
		raSetRecord(tf_os_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//广告位报表-----------------------
		tf_esd_key := "MEDIAADID^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + esd + "^" + slotid + "^" + domain
		raSetRecord_time(tf_esd_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, alive_time, is_two_jump)
		//地域报表-----------------------
		tf_area_key := "PLACE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + areaid
		raSetRecord(tf_area_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//域名报表-----------------------
		tf_domain_key := "MEDIA^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain
		raSetRecord_time(tf_domain_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, alive_time, is_two_jump)
		//ExchangeID报表-----------------------
		tf_exid_key := "EXANGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + exchangeid
		raSetRecord(tf_exid_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//频道报表-----------------------
		if len(channel) > 0 || len(cs) > 0 {
			if len(channel) == 0 {
				channel = "-1"
			}
			if len(cs) == 0 {
				cs = "-1"
			}
			tf_ch_key := "CHANNEL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + channel + "^" + cs + "^" + exchangeid
			raSetRecord(tf_ch_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		}
		//媒体分类报表-----------------------
		tf_mt_key := "CONTENT^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + media_type + "^" + exchangeid
		raSetRecord(tf_mt_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//素材报表-----------------------
		tf_material_key := "MATERIAL^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid
		raSetRecord(tf_material_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//素材报表 小时-----------------------
		tf_materialh_key := "MATERIALH^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + adid + "^" + str_hour
		raSetRecord(tf_materialh_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//域名分类-------------------
		tf_dc_key := "DOMAINCATE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + domain_category
		raSetRecord(tf_dc_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		//人群报表-------------------
		if len(user_cate) > 0 {
			tf_crowd_key := "CROWD^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_cate
			raSetRecord(tf_crowd_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		}
		//性别报表-------------------
		if len(user_gender) > 0 {
			tf_gender_key := "GENDER^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_gender
			raSetRecord(tf_gender_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		}
		//年龄报表-------------------
		if len(user_age) > 0 {
			tf_age_key := "AGE^" + str_today + "^" + advid + "^" + cid + "^" + oid + "^" + user_age
			raSetRecord(tf_age_key, oid, cid, advid, algorithm_type, is_umod, int64(kclick_cost), int64(advcost), isarrive, is_0_arrive, is_user_arrive, is_user_action, mbeahivor, 0)
		}
		//---------------------------------
		shift_g_recored_lock.RUnlock()
	}
}

func updateRecord() {
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
}

func updateMap2Redis(idx int, rec_time int64) {
	redis_conn := pools_redis[idx].Get()
	defer redis_conn.Close()
	var EDP_S []map[string]string
	var EDP_D []map[string]string
	now_timestamp := time.Now().Unix()
	for redis_key, rpinfo := range g_recored[idx].m_record {
		var strarr []string = strings.Split(redis_key, "^")

		res, err := redis_conn.Do("HMSET", redis_key, "order_id", rpinfo.order_id, "campid", rpinfo.camp_id, "adAccountName", rpinfo.advert_id, "algorithm_type", rpinfo.algorithm_type)
		if err != nil {
			fmt.Println("ErrHs:", res, "HMSET", redis_key, "order_id", rpinfo.order_id, "campid", rpinfo.camp_id, "adAccountName", rpinfo.advert_id, "algorithm_type", rpinfo.algorithm_type)
		}
		res, err = redis_conn.Do("EXPIRE", redis_key, 172800)
		if err != nil {
			fmt.Println("ErrHs:", res, "EXPIRE", redis_key, 172800)
		}

		var tmp_num int64
		tmp_num = 0

		if rpinfo.shopcost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "shopcost", rpinfo.shopcost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "shopcost", rpinfo.shopcost)
			}
		}
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
		}
		if rpinfo.arrive_num > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "arrive", rpinfo.arrive_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "arrive", rpinfo.arrive_num)
			}
		}

		if rpinfo.true_cost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "realprice", rpinfo.true_cost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "realprice", rpinfo.true_cost)
			}
		}
		if rpinfo.sys_cost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "private_price", rpinfo.sys_cost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "private_price", rpinfo.sys_cost)
			}
		}
		if rpinfo.adv_cost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "open_price", rpinfo.adv_cost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "open_price", rpinfo.adv_cost)
			}
			if strarr[0] == "TF" && len(strarr) == 5 && (rpinfo.algorithm_type == "0" || rpinfo.algorithm_type == "1") {
				redis_conn.Do("HMSET", redis_key, "minute_cost", ((60 * rpinfo.adv_cost) / rec_time), "mc_rec_time", now_timestamp)
			}
		}
		if rpinfo.click_cost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "click_price", rpinfo.click_cost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "click_price", rpinfo.click_cost)
			}
		}
		if rpinfo.kclick_cost > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "kclick_price", rpinfo.kclick_cost))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "kclick_price", rpinfo.kclick_cost)
			}
			if strarr[0] == "TF" && len(strarr) == 5 && (rpinfo.algorithm_type == "2" || rpinfo.algorithm_type == "3") {
				redis_conn.Do("HMSET", redis_key, "minute_cost", ((60 * rpinfo.adv_cost) / rec_time), "mc_rec_time", now_timestamp)
			}
		}
		if rpinfo.user_click_num > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "userClick", rpinfo.user_click_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "userClick", rpinfo.user_click_num)
			}
		}
		if rpinfo.user_arrive_num > 0 {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "userReach", rpinfo.user_arrive_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "userReach", rpinfo.user_arrive_num)
			}
		}
		if rpinfo.two_jump_num > 0 && (strings.Contains(redis_key, "MEDIAADID") || strings.Contains(redis_key, "MEDIA")) {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "twojumpnum", rpinfo.two_jump_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "twojumpnum", rpinfo.two_jump_num)
			}
		}
		if rpinfo.alive_time > 0 && (strings.Contains(redis_key, "MEDIAADID") || strings.Contains(redis_key, "MEDIA")) {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "alive_time", rpinfo.alive_time))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "alive_time", rpinfo.alive_time)
			}
		}
		if rpinfo.alive_num > 0 && (strings.Contains(redis_key, "MEDIAADID") || strings.Contains(redis_key, "MEDIA")) {
			tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, "alive_num", rpinfo.alive_num))
			if err != nil {
				fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, "alive_num", rpinfo.alive_num)
			}
		}

		for at_key, at_num := range rpinfo.action_map {
			if at_num > 0 {
				tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, ("at_" + at_key), at_num))
				if err != nil {
					fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, ("at_" + at_key), at_num)
				}
			}
		}

		for uat_key, uat_num := range rpinfo.user_action_map {
			if uat_num > 0 {
				tmp_num, err = redis.Int64(redis_conn.Do("HINCRBY", redis_key, ("ut_" + uat_key), uat_num))
				if err != nil {
					fmt.Println("ErrInc:", tmp_num, err, "HINCRBY", redis_key, ("ut_" + uat_key), uat_num)
				}
			}
		}

		if is_backup == false && !strings.Contains(redis_key, "MEDIA^") && !strings.Contains(redis_key, "MEDIAADID^") && !strings.Contains(redis_key, "ATS^") && !strings.Contains(redis_key, "ATD^") {
			if false == requestHttp(redis_key) {
				fmt.Println("requestHttp ,key:", redis_key, ",statues: failed")
			}
		}

		if strings.Contains(redis_key, "MEDIAADID^") {
			tmp_map := make(map[string]string)
			tmp_map, err = redis.StringMap(redis_conn.Do("HGETALL", redis_key))
			if err != nil {
				fmt.Println("ErrInc:", tmp_map, "HGETALL", redis_key)
				continue
			}
			tmp_map["key"] = redis_key
			EDP_S = append(EDP_S, tmp_map)
		} else if strings.Contains(redis_key, "MEDIA^") {
			tmp_map := make(map[string]string)
			tmp_map, err = redis.StringMap(redis_conn.Do("HGETALL", redis_key))
			if err != nil {
				fmt.Println("ErrInc:", tmp_map, "HGETALL", redis_key)
				continue
			}
			tmp_map["key"] = redis_key
			EDP_D = append(EDP_D, tmp_map)
		}
	}
	g_recored[idx].m_record = make(map[string]*reportInfo)
	edp_S_obj_str, _ := json.Marshal(EDP_S)
	edp_D_obj_str, _ := json.Marshal(EDP_D)

	redis_conn_edp := pools_redis_edp.Get()
	defer redis_conn_edp.Close()

	if len(edp_S_obj_str) > 0 && (is_backup == false) {
		res, err := redis_conn_edp.Do("RPUSH", "EDP_S", edp_S_obj_str)
		if err != nil {
			fmt.Println("Err RPUSH EDP_S", err, res)
		}
	}
	if len(edp_D_obj_str) > 0 && (is_backup == false) {
		res, err := redis_conn_edp.Do("RPUSH", "EDP_D", edp_D_obj_str)
		if err != nil {
			fmt.Println("Err RPUSH EDP_D", err, res)
		}
	}
}

func isOrderStop(oid string) bool {
	redis_conn := pools_redis_stopnotic.Get()
	defer redis_conn.Close()
	isexist, err := redis.Bool(redis_conn.Do("EXISTS", "f_"+oid))
	if err != nil {
		fmt.Println("Err:", "EXISTS", "f_"+oid, "  IsExists is", isexist, " ,err:", err)
		isexist = false
	}
	return isexist
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

func main() {
	go loadblacklist()
	flag.Parse()
	str_is_backup := flag.Arg(0)
	log_resource := flag.Arg(1)
	if str_is_backup == "back" {
		is_backup = true
	} else {
		is_backup = false
	}
	pools_redis = append(pools_redis, newPool("127.0.0.1:6373"), newPool("127.0.0.1:6373"))
	pools_redis_click = append(pools_redis_click, newPool("127.0.0.1:6374"), newPool("127.0.0.1:6374"))
	pools_redis_sid = append(pools_redis_sid, newPool("127.0.0.1:6378"), newPool("127.0.0.1:6378"))
	pools_redis_dcate = append(pools_redis_dcate, newPool("127.0.0.1:6376"), newPool("127.0.0.1:6376"))
	pools_redis_mediabuy = append(pools_redis_mediabuy, newPool("127.0.0.1:6372"), newPool("127.0.0.1:6372"))
	pools_redis_stopnotic = newPool("127.0.0.1:6367")
	pools_redis_edp = newPool("127.0.0.1:6366")
	pools_redis_clickinfo = newPool("127.0.0.1:6365")

	clearMediaBuyInfo()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var quit chan int
	initRecoredSet()
	initTaskCh()
	initDomainCateMap()
	if log_resource == "syslog" {
		go fillTask_syslog()
	} else {
		go fillTask_kafka()
	}
	for i := 0; i < count; i++ {
		go processTask(i)
	}
	go thread_arrive_process()
	go updateRecord()
	go reloadDomainCateMap()
	go thread_order_shop()

	<-quit
}

func thread_order_shop() {
	var num int = 0
	for {
		tmp_str, ok := <-order_shop
		if ok == false {
			fmt.Fprintln(os.Stderr, "order_shop closed")
		}

		if num == 0 {
			num = 1
		} else {
			num = 0
		}
		redis_conn := pools_redis[num].Get()
		res, err := redis_conn.Do("LPUSH", "order_shop", tmp_str)
		if err != nil {
			fmt.Fprintln(os.Stderr, "LPUSH", res, err)
		}
		redis_conn.Close()
	}
}

func loadblacklist() {
	for {
		loadfile()
		time.Sleep(3600 * time.Second)
	}
}

func loadfile() {
	inputFile, inputError := os.Open("ipblacklist.conf")
	if inputError != nil {
		fmt.Fprintln(os.Stderr, time.Now(), "An error occurred on opening: ipblacklist.conf", inputError)
		return
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)
	blacklistip_map_lock.Lock()
	blacklistip_map = make(map[string]int)
	for {
		inputString, readerError := inputReader.ReadString('\n')
		inputString = strings.Replace(inputString, "\n", "", 1)
		inputString = strings.Replace(inputString, "\r", "", 1)
		inputString = strings.Replace(inputString, "\t", "", 1)
		if readerError == io.EOF {
			break
		}
		if len(inputString) > 0 {
			blacklistip_map[inputString] = 1
		}
	}
	blacklistip_map_lock.Unlock()
}
