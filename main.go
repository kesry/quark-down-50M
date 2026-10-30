package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================
// 常量
// ============================================================

// 超过此大小的文件，download 接口无法获取下载链接，直接跳过
const maxDownloadSize = 50 * 1024 * 1024 // 50 MB

// 最大并发下载数
const maxConcurrent = 4

// ============================================================
// 配置
// ============================================================

type Config struct {
	Cookie              string `json:"cookie"`
	PdirFid             string `json:"pdir_fid"`
	DownloadDir         string `json:"download_dir"`
	PageSize            int    `json:"page_size"`
	BatchSize           int    `json:"batch_size"`
	TimeoutSec          int    `json:"timeout_sec"`
	DeleteAfterDownload bool   `json:"delete_after_download"`
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置失败: %w", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	c.Cookie = strings.TrimSpace(c.Cookie)
	c.PdirFid = strings.TrimSpace(c.PdirFid)
	c.DownloadDir = strings.TrimSpace(c.DownloadDir)

	if c.Cookie == "" || c.PdirFid == "" || c.DownloadDir == "" {
		return nil, fmt.Errorf("配置缺少必填项 (cookie / pdir_fid / download_dir)")
	}
	if c.PageSize <= 0 {
		c.PageSize = 100
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	if c.TimeoutSec <= 0 {
		c.TimeoutSec = 120
	}
	return &c, nil
}

// ============================================================
// 夸克 API 客户端
// ============================================================

const (
	baseURL      = "https://drive-pc.quark.cn"
	sortPath     = "/1/clouddrive/file/sort"
	downloadPath = "/1/clouddrive/file/download"
	deletePath   = "/1/clouddrive/file/delete"

	userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
	referer   = "https://pan.quark.cn/"
	origin    = "https://pan.quark.cn"
)

type Client struct {
	http   *http.Client
	cookie string
}

func NewClient(cookie string, timeoutSec int) *Client {
	return &Client{
		http:   &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		cookie: cookie,
	}
}

func (c *Client) newReq(method, rawURL string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json, text/plain, */*")
	req.Header.Set("accept-language", "zh-CN,zh;q=0.9")
	req.Header.Set("cookie", c.cookie)
	req.Header.Set("origin", origin)
	req.Header.Set("referer", referer)
	req.Header.Set("user-agent", userAgent)
	req.Header.Set("sec-ch-ua", `"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"Windows"`)
	req.Header.Set("sec-fetch-dest", "empty")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("sec-fetch-site", "same-site")
	return req, nil
}

// ---------------- sort ----------------

type FileItem struct {
	Fid      string `json:"fid"`
	FileName string `json:"file_name"`
	Size     int64  `json:"size"`
	Dir      bool   `json:"dir"`
	File     bool   `json:"file"`
}

type sortResp struct {
	Status int    `json:"status"`
	Code   int    `json:"code"`
	Msg    string `json:"message"`
	Data   struct {
		List []FileItem `json:"list"`
	} `json:"data"`
}

func (c *Client) ListFiles(pdirFid string, pageSize int) ([]FileItem, error) {
	q := url.Values{}
	q.Set("pr", "ucpro")
	q.Set("fr", "pc")
	q.Set("uc_param_str", "")
	q.Set("pdir_fid", pdirFid)
	q.Set("_page", "1")
	q.Set("_size", fmt.Sprint(pageSize))
	q.Set("_fetch_total", "1")
	q.Set("_fetch_sub_dirs", "0")
	q.Set("_sort", "file_type:asc,updated_at:desc")
	q.Set("fetch_all_file", "1")
	q.Set("fetch_risk_file_name", "1")

	req, err := c.newReq("GET", baseURL+sortPath+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("cache-control", "no-cache")
	req.Header.Set("pragma", "no-cache")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var sr sortResp
	if err := json.Unmarshal(body, &sr); err != nil {
		return nil, fmt.Errorf("解析 sort 返回失败: %w, body=%s", err, string(body))
	}
	if sr.Code != 0 {
		return nil, fmt.Errorf("sort 接口异常 code=%d msg=%s", sr.Code, sr.Msg)
	}

	var out []FileItem
	for _, f := range sr.Data.List {
		if f.File && !f.Dir {
			out = append(out, f)
		}
	}
	return out, nil
}

// ---------------- download ----------------

type dlItem struct {
	Fid         string `json:"fid"`
	FileName    string `json:"file_name"`
	Size        int64  `json:"size"`
	DownloadURL string `json:"download_url"`
}

type dlResp struct {
	Status int      `json:"status"`
	Code   int      `json:"code"`
	Msg    string   `json:"message"`
	Data   []dlItem `json:"data"`
}

func (c *Client) GetDownloadURLs(fids []string) ([]dlItem, error) {
	payload, _ := json.Marshal(map[string][]string{"fids": fids})
	u := baseURL + downloadPath + "?pr=ucpro&fr=pc&uc_param_str="
	req, err := c.newReq("POST", u, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/json;charset=UTF-8")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var dr dlResp
	if err := json.Unmarshal(body, &dr); err != nil {
		return nil, fmt.Errorf("解析 download 返回失败: %w, body=%s", err, string(body))
	}
	if dr.Code != 0 {
		return nil, fmt.Errorf("download 接口异常 code=%d msg=%s", dr.Code, dr.Msg)
	}

	var out []dlItem
	for _, it := range dr.Data {
		if it.DownloadURL != "" {
			out = append(out, it)
		}
	}
	return out, nil
}

// ---------------- delete ----------------

type delResp struct {
	Status int    `json:"status"`
	Code   int    `json:"code"`
	Msg    string `json:"message"`
	Data   struct {
		TaskID string `json:"task_id"`
		Finish bool   `json:"finish"`
	} `json:"data"`
}

// DeleteFiles 删除云端文件。action_type=2 表示删除到回收站（与抓包一致）
func (c *Client) DeleteFiles(fids []string) error {
	if len(fids) == 0 {
		return nil
	}
	payload := map[string]any{
		"action_type":  2,
		"filelist":     fids,
		"exclude_fids": []string{},
	}
	body, _ := json.Marshal(payload)

	u := baseURL + deletePath + "?pr=ucpro&fr=pc&uc_param_str="
	req, err := c.newReq("POST", u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var dr delResp
	if err := json.Unmarshal(respBody, &dr); err != nil {
		return fmt.Errorf("解析 delete 返回失败: %w, body=%s", err, string(respBody))
	}
	if dr.Code != 0 {
		return fmt.Errorf("delete 接口异常 code=%d msg=%s", dr.Code, dr.Msg)
	}
	return nil
}

// ============================================================
// 工具函数
// ============================================================

var illegalChars = regexp.MustCompile(`[\x00-\x1f<>:"|?*]`)

func sanitizeName(name, fallback string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = illegalChars.ReplaceAllString(name, "_")
	name = strings.TrimSpace(strings.Trim(name, "."))
	if name == "" {
		name = fallback
	}
	return name
}

func humanSize(n int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	f := float64(n)
	for _, u := range units {
		if f < 1024 || u == units[len(units)-1] {
			return fmt.Sprintf("%.2f %s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%d B", n)
}

// ============================================================
// 下载
// ============================================================

// Download 下载文件到 dir。带断点续传 + 进度显示。
// tag 用于多并发时区分进度行（例如序号）。
func (c *Client) Download(rawURL, dir, rawName, fid string, expectedSize int64, tag string) (string, error) {
	safe := sanitizeName(rawName, fid)
	target := filepath.Join(dir, safe)
	tmp := target + ".part"

	var existing int64
	if st, err := os.Stat(tmp); err == nil {
		existing = st.Size()
	}

	req, err := c.newReq("GET", rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "zh-CN,zh;q=0.9")
	if existing > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existing))
	}

	dlClient := &http.Client{Timeout: 0}
	resp, err := dlClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}

	var mode string
	var written int64
	if resp.StatusCode == http.StatusPartialContent && existing > 0 {
		mode = "ab"
		written = existing
	} else {
		mode = "wb"
		written = 0
		existing = 0
	}

	total := expectedSize
	if total == 0 && resp.ContentLength > 0 {
		total = resp.ContentLength + existing
	}

	flags := os.O_CREATE | os.O_WRONLY
	if mode == "ab" {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(tmp, flags, 0o644)
	if err != nil {
		return "", err
	}

	start := time.Now()
	lastPrint := time.Now()
	buf := make([]byte, 256*1024)

	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				f.Close()
				return "", werr
			}
			written += int64(n)
			if time.Since(lastPrint) > 500*time.Millisecond {
				lastPrint = time.Now()
				elapsed := time.Since(start).Seconds()
				if elapsed <= 0 {
					elapsed = 1
				}
				speed := float64(written-int64(existing)) / elapsed
				if total > 0 {
					fmt.Printf("[%s] 进度 %.2f%%  %s/%s  %s/s   \n",
						tag,
						float64(written)/float64(total)*100,
						humanSize(written), humanSize(total), humanSize(int64(speed)))
				} else {
					fmt.Printf("[%s] 已下载 %s  %s/s   \n",
						tag, humanSize(written), humanSize(int64(speed)))
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			f.Close()
			return "", rerr
		}
	}
	f.Close()

	if err := os.Rename(tmp, target); err != nil {
		return "", err
	}
	return target, nil
}

// ============================================================
// 文件锁
// ============================================================

func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("已有实例在运行")
	}
	return f, nil
}

// ============================================================
// 帮助 / 版本
// ============================================================

const version = "1.1.0"

const usageText = `quark-sync - 夸克网盘目录增量下载 + 下载完删除云端文件

用法:
  quark-sync [选项]

选项:
  -config string
        配置文件路径（JSON 格式）。
        优先级: -config > 环境变量 QUARK_CONFIG > $HOME/.quark-dl/config.json

  -h, -help, --help
        显示本帮助并退出。

  -v, -version, --version
        显示版本号并退出。

  -list
        只列出云端目录下的文件（fid / 大小 / 文件名），不下载不删除。

  -dry-run
        演练模式：只打印将要下载 / 将要删除的文件，不实际执行。

行为说明:
  * 超过 50 MB 的文件会被自动跳过（download 接口不返回链接）。
  * 最多 4 个文件同时下载。
  * 下载成功后删除云端（delete_after_download=true 时，删除到回收站）。
  * 下载失败保留云端，下轮自动重试；半成品 .part 支持断点续传。
  * 本地已存在且大小一致的文件会跳过下载，直接清理云端。

配置文件 (config.json) 示例:
  {
    "cookie": "b-user-id=...; _UP_A4A_11_=...; ...",
    "pdir_fid": "4b55cc30da6d4ca2867a945314e1a542",
    "download_dir": "/home/yourname/music/quark",
    "page_size": 100,
    "batch_size": 50,
    "timeout_sec": 120,
    "delete_after_download": true
  }

字段说明:
  cookie                  必填。浏览器里复制出来的完整 cookie。
  pdir_fid                必填。要监控的云端目录 fid。
  download_dir            必填。本地保存目录（绝对路径）。
  page_size               可选，默认 100。sort 接口每页数量。
  batch_size              可选，默认 50。每批请求下载链接的 fid 数。
  timeout_sec             可选，默认 120。API 请求超时秒数（不影响下载）。
  delete_after_download   可选，默认 true。下载成功后是否删除云端文件。

示例:
  quark-sync                                  # 用默认配置跑一轮
  quark-sync -config /etc/quark-sync.json     # 指定配置
  quark-sync -list                            # 只看目录里有什么
  quark-sync -dry-run                         # 演练，不实际下载/删除

定时任务 (cron):
  * * * * * /home/yourname/.quark-dl/quark-sync \
      -config /home/yourname/.quark-dl/config.json \
      >> /home/yourname/.quark-dl/quark-sync.log 2>&1
`

func printUsage() {
	fmt.Fprint(os.Stderr, usageText)
}

// ============================================================
// main
// ============================================================

func defaultConfigPath() string {
	if p := os.Getenv("QUARK_CONFIG"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.json"
	}
	return filepath.Join(home, ".quark-dl", "config.json")
}

// 统计计数
type stats struct {
	downloaded  atomic.Int64
	skipped     atomic.Int64
	deleted     atomic.Int64
	failed      atomic.Int64
	tooLarge    atomic.Int64
}

func main() {
	var (
		configPath  string
		showHelp    bool
		showVersion bool
		dryRun      bool
		listOnly    bool
	)

	flag.Usage = printUsage

	flag.StringVar(&configPath, "config", defaultConfigPath(), "配置文件路径（JSON）")
	flag.BoolVar(&showHelp, "h", false, "显示帮助")
	flag.BoolVar(&showHelp, "help", false, "显示帮助")
	flag.BoolVar(&showVersion, "v", false, "显示版本号")
	flag.BoolVar(&showVersion, "version", false, "显示版本号")
	flag.BoolVar(&dryRun, "dry-run", false, "只演练，不实际下载/删除")
	flag.BoolVar(&listOnly, "list", false, "只列出目录下的文件")

	flag.Parse()

	if showHelp {
		printUsage()
		os.Exit(0)
	}
	if showVersion {
		fmt.Println("quark-sync version", version)
		os.Exit(0)
	}
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "未知参数: %v\n\n", flag.Args())
		printUsage()
		os.Exit(2)
	}

	log.SetFlags(log.LstdFlags)

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("[!] %v（用 -h 查看帮助）", err)
	}

	if err := os.MkdirAll(cfg.DownloadDir, 0o755); err != nil {
		log.Fatalf("[!] 创建下载目录失败: %v", err)
	}

	// 单实例锁
	lockPath := filepath.Join(cfg.DownloadDir, ".quark-sync.lock")
	lock, err := acquireLock(lockPath)
	if err != nil {
		log.Printf("跳过本次: %v", err)
		return
	}
	defer lock.Close()

	client := NewClient(cfg.Cookie, cfg.TimeoutSec)

	// ---------- 拉列表 ----------
	log.Printf("==> 拉取文件列表 (pdir_fid=%s)", cfg.PdirFid)
	files, err := client.ListFiles(cfg.PdirFid, cfg.PageSize)
	if err != nil {
		log.Fatalf("[!] 拉取文件列表失败: %v", err)
	}
	if len(files) == 0 {
		log.Printf("==> 目录下没有文件")
		return
	}
	log.Printf("==> 目录下共 %d 个文件待处理", len(files))

	// ---------- -list 模式 ----------
	if listOnly {
		fmt.Printf("  %-34s %12s  %s\n", "FID", "SIZE", "NAME")
		fmt.Println(strings.Repeat("-", 82))
		for _, f := range files {
			mark := " "
			if f.Size > maxDownloadSize {
				mark = "!"
			}
			fmt.Printf("%s %-34s %12s  %s\n", mark, f.Fid, humanSize(f.Size), f.FileName)
		}
		fmt.Println()
		fmt.Printf("提示: 带 ! 的文件超过 %s，download 接口无法获取链接，会被跳过。\n",
			humanSize(maxDownloadSize))
		return
	}

	// ---------- 收集待处理任务 ----------
	type task struct {
		item     dlItem
		meta     FileItem
		safeName string
		size     int64
	}

	var (
		tasks  []task
		st     stats
	)

	// 先过滤超大文件，再分批拿下载链接
	for i := 0; i < len(files); i += cfg.BatchSize {
		end := i + cfg.BatchSize
		if end > len(files) {
			end = len(files)
		}
		batch := files[i:end]

		var usable []FileItem
		for _, f := range batch {
			if f.Size > maxDownloadSize {
				log.Printf("==> 跳过（超过 %s，接口不支持）: %s (%s)",
					humanSize(maxDownloadSize), f.FileName, humanSize(f.Size))
				st.tooLarge.Add(1)
				continue
			}
			usable = append(usable, f)
		}
		if len(usable) == 0 {
			continue
		}

		fids := make([]string, 0, len(usable))
		metaByFid := make(map[string]FileItem, len(usable))
		for _, f := range usable {
			fids = append(fids, f.Fid)
			metaByFid[f.Fid] = f
		}

		items, err := client.GetDownloadURLs(fids)
		if err != nil {
			log.Printf("[!] 获取下载链接失败: %v", err)
			st.failed.Add(int64(len(usable)))
			continue
		}

		for _, it := range items {
			meta := metaByFid[it.Fid]
			name := it.FileName
			if name == "" {
				name = meta.FileName
			}
			size := it.Size
			if size == 0 {
				size = meta.Size
			}
			if size > maxDownloadSize {
				log.Printf("==> 跳过（超过 %s）: %s (%s)",
					humanSize(maxDownloadSize), name, humanSize(size))
				st.tooLarge.Add(1)
				continue
			}
			tasks = append(tasks, task{
				item:     it,
				meta:     meta,
				safeName: sanitizeName(name, it.Fid),
				size:     size,
			})
		}
	}

	if len(tasks) == 0 {
		log.Printf("==> 没有需要下载的文件")
		logSummary(&st)
		return
	}

	// ---------- dry-run ----------
	if dryRun {
		for _, t := range tasks {
			target := filepath.Join(cfg.DownloadDir, t.safeName)
			skip := false
			if stt, err := os.Stat(target); err == nil {
				if t.size == 0 || stt.Size() == t.size {
					skip = true
				}
			}
			if skip {
				log.Printf("[dry-run] 跳过下载（本地已有），将删除云端: %s (%s)", t.safeName, t.item.Fid)
			} else {
				log.Printf("[dry-run] 将下载: %s (%s)", t.safeName, humanSize(t.size))
				if cfg.DeleteAfterDownload {
					log.Printf("[dry-run] 下载成功后将删除云端: %s (%s)", t.safeName, t.item.Fid)
				}
			}
		}
		logSummary(&st)
		return
	}

	// ---------- 并发下载 ----------
	log.Printf("==> 开始下载，并发数 %d，共 %d 个文件", maxConcurrent, len(tasks))

	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for idx, t := range tasks {
		wg.Add(1)
		sem <- struct{}{} // 获取令牌，满了就阻塞

		go func(idx int, t task) {
			defer wg.Done()
			defer func() { <-sem }() // 释放令牌

			tag := fmt.Sprintf("%d/%d", idx+1, len(tasks))

			// 本地已存在且大小一致 → 跳过下载，直接删云端
			target := filepath.Join(cfg.DownloadDir, t.safeName)
			skipDownload := false
			if stt, err := os.Stat(target); err == nil {
				if t.size == 0 || stt.Size() == t.size {
					log.Printf("[%s] 本地已存在，跳过下载: %s", tag, t.safeName)
					skipDownload = true
					st.skipped.Add(1)
				}
			}

			if !skipDownload {
				log.Printf("[%s] 开始下载: %s (%s)", tag, t.safeName, humanSize(t.size))
				start := time.Now()
				path, derr := client.Download(t.item.DownloadURL, cfg.DownloadDir, t.safeName, t.item.Fid, t.size, tag)
				if derr != nil {
					log.Printf("[%s] [!] 下载失败 %s: %v（保留云端，下轮重试）", tag, t.safeName, derr)
					st.failed.Add(1)
					return
				}
				log.Printf("[%s] 完成: %s  用时 %s", tag, path, time.Since(start).Round(time.Second))
				st.downloaded.Add(1)
			}

			// 删云端
			if cfg.DeleteAfterDownload {
				if derr := client.DeleteFiles([]string{t.item.Fid}); derr != nil {
					log.Printf("[%s] [!] 删除云端失败 %s: %v", tag, t.safeName, derr)
					st.failed.Add(1)
				} else {
					log.Printf("[%s] 已删除云端: %s", tag, t.safeName)
					st.deleted.Add(1)
				}
			}
		}(idx, t)
	}

	wg.Wait()
	logSummary(&st)
}

func logSummary(st *stats) {
	log.Printf("==> 本轮结束：下载 %d，跳过(已存在) %d，跳过(超 %s) %d，删除 %d，失败 %d",
		st.downloaded.Load(),
		st.skipped.Load(),
		humanSize(maxDownloadSize),
		st.tooLarge.Load(),
		st.deleted.Load(),
		st.failed.Load(),
	)
}
