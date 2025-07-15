package smart

import (
    "encoding/json"
    "math"
    "sort"
    "strings"
    "time"
    "errors"
    "fmt"
    "hash/fnv"
    "sync"
    "sync/atomic"
    "container/heap"

    "github.com/metacubex/mihomo/log"
)

var (
    shardedLocks [1024]*sync.RWMutex
    shardedLocksOnce sync.Once
)

type AtomicStatsRecord struct {
    success     int64
    failure     int64
    connectTime int64
    latency     int64
    lastUsed    int64
    
    mu            sync.RWMutex
    weights       map[string]float64
    uploadTotal   float64
    downloadTotal float64
    duration      float64
}

type AtomicRecordManager struct {
    records sync.Map
}

type domainLastUsed struct {
    domain   string
    lastUsed time.Time
    types    []string
}

type domainMinHeap []domainLastUsed

type asnLastUsed struct {
    asn      string
    lastUsed time.Time
    types    []string
}

type asnMinHeap []asnLastUsed

var (
    globalAtomicManager *AtomicRecordManager
    atomicManagerOnce   sync.Once
)

func initShardedLocks() {
    shardedLocksOnce.Do(func() {
        for i := range shardedLocks {
            shardedLocks[i] = &sync.RWMutex{}
        }
    })
}

// 域名节点锁
func GetDomainNodeLock(domain, group, proxyName string) *sync.RWMutex {
    initShardedLocks()
    
    h := fnv.New32a()
    h.Write([]byte(domain))
    h.Write([]byte(group))
    h.Write([]byte(proxyName))
    hash := h.Sum32()
    
    return shardedLocks[hash&1023]
}

func GetAtomicManager() *AtomicRecordManager {
    atomicManagerOnce.Do(func() {
        globalAtomicManager = &AtomicRecordManager{}
    })
    return globalAtomicManager
}

// 获取或创建原子记录
func (m *AtomicRecordManager) GetOrCreateAtomicRecord(cacheKey string, store *Store, groupName, configName, domain, proxyName string) *AtomicStatsRecord {
    if value, ok := m.records.Load(cacheKey); ok {
        return value.(*AtomicStatsRecord)
    }
    
    record := &AtomicStatsRecord{
        weights: make(map[string]float64),
    }
    atomic.StoreInt64(&record.lastUsed, time.Now().Unix())
    
    if store != nil {
        if existingData, err := store.GetStatsForDomain(groupName, configName, domain); err == nil {
            if data, exists := existingData[proxyName]; exists {
                var existingRecord StatsRecord
                if json.Unmarshal(data, &existingRecord) == nil {
                    atomic.StoreInt64(&record.success, existingRecord.Success)
                    atomic.StoreInt64(&record.failure, existingRecord.Failure)
                    atomic.StoreInt64(&record.connectTime, existingRecord.ConnectTime)
                    atomic.StoreInt64(&record.latency, existingRecord.Latency)
                    atomic.StoreInt64(&record.lastUsed, existingRecord.LastUsed.Unix())
                    
                    record.mu.Lock()
                    for k, v := range existingRecord.Weights {
                        record.weights[k] = v
                    }
                    record.uploadTotal = existingRecord.UploadTotal
                    record.downloadTotal = existingRecord.DownloadTotal
                    record.duration = existingRecord.ConnectionDuration
                    record.mu.Unlock()
                }
            }
        }
    }
    
    actual, loaded := m.records.LoadOrStore(cacheKey, record)
    if loaded {
        return actual.(*AtomicStatsRecord)
    }
    
    return record
}

// 创建统计快照
func (record *AtomicStatsRecord) CreateStatsSnapshot() *StatsRecord {
    if record == nil {
        return &StatsRecord{
            Weights: make(map[string]float64),
        }
    }
    
    success := record.Get("success")
    failure := record.Get("failure")
    connectTime := record.Get("connectTime")
    latency := record.Get("latency")
    lastUsed := record.Get("lastUsed")
    uploadTotal := record.Get("uploadTotal")
    downloadTotal := record.Get("downloadTotal")
    duration := record.Get("duration")
    weights := record.Get("weights")
    
    var successVal, failureVal, connectTimeVal, latencyVal, lastUsedVal int64
    var uploadTotalVal, downloadTotalVal, durationVal float64
    var weightsMap map[string]float64
    
    if success != nil {
        if val, ok := success.(int64); ok {
            successVal = val
        }
    }
    
    if failure != nil {
        if val, ok := failure.(int64); ok {
            failureVal = val
        }
    }
    
    if connectTime != nil {
        if val, ok := connectTime.(int64); ok {
            connectTimeVal = val
        }
    }
    
    if latency != nil {
        if val, ok := latency.(int64); ok {
            latencyVal = val
        }
    }
    
    if lastUsed != nil {
        if val, ok := lastUsed.(int64); ok {
            lastUsedVal = val
        }
    }
    
    if uploadTotal != nil {
        if val, ok := uploadTotal.(float64); ok {
            uploadTotalVal = val
        }
    }
    
    if downloadTotal != nil {
        if val, ok := downloadTotal.(float64); ok {
            downloadTotalVal = val
        }
    }
    
    if duration != nil {
        if val, ok := duration.(float64); ok {
            durationVal = val
        }
    }
    
    if weights != nil {
        if val, ok := weights.(map[string]float64); ok {
            weightsMap = val
        } else {
            weightsMap = make(map[string]float64)
        }
    } else {
        weightsMap = make(map[string]float64)
    }
    
    return &StatsRecord{
        Success:            successVal,
        Failure:            failureVal,
        ConnectTime:        connectTimeVal,
        Latency:            latencyVal,
        LastUsed:           time.Unix(lastUsedVal, 0),
        Weights:            weightsMap,
        UploadTotal:        uploadTotalVal,
        DownloadTotal:      downloadTotalVal,
        ConnectionDuration: durationVal,
    }
}

func (r *AtomicStatsRecord) Get(field string) interface{} {
    switch field {
    case "success":
        return atomic.LoadInt64(&r.success)
    case "failure":
        return atomic.LoadInt64(&r.failure)
    case "connectTime":
        return atomic.LoadInt64(&r.connectTime)
    case "latency":
        return atomic.LoadInt64(&r.latency)
    case "lastUsed":
        return atomic.LoadInt64(&r.lastUsed)
    case "weights":
        r.mu.RLock()
        defer r.mu.RUnlock()
        if r.weights == nil {
            return nil
        }
        result := make(map[string]float64, len(r.weights))
        for k, v := range r.weights {
            result[k] = v
        }
        return result
    case "uploadTotal":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.uploadTotal
    case "downloadTotal":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.downloadTotal
    case "duration":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.duration
    default:
        return nil
    }
}

func (r *AtomicStatsRecord) Set(field string, value interface{}) {
    switch field {
    case "success":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.success, v)
        }
    case "failure":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.failure, v)
        }
    case "connectTime":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.connectTime, v)
        }
    case "latency":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.latency, v)
        }
    case "lastUsed":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.lastUsed, v)
        }
    case "uploadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.uploadTotal = v
        }
    case "downloadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.downloadTotal = v
        }
    case "duration":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.duration = v
        }
    }
}

func (r *AtomicStatsRecord) Add(field string, value interface{}) {
    switch field {
    case "success":
        if v, ok := value.(int64); ok {
            atomic.AddInt64(&r.success, v)
        }
    case "failure":
        if v, ok := value.(int64); ok {
            atomic.AddInt64(&r.failure, v)
        }
    case "uploadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.uploadTotal += v
        }
    case "downloadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.downloadTotal += v
        }
    }
}

// 权重相关的特殊方法
func (r *AtomicStatsRecord) GetWeight(weightType string) float64 {
    r.mu.RLock()
    defer r.mu.RUnlock()
    
    if r.weights == nil {
        return 0
    }
    return r.weights[weightType]
}

func (r *AtomicStatsRecord) SetWeight(weightType string, value float64) {
    r.mu.Lock()
    defer r.mu.Unlock()
    
    if r.weights == nil {
        r.weights = make(map[string]float64)
    }
    r.weights[weightType] = value
}

// 获取节点权重排名
func (s *Store) GetNodeWeightRanking(group, config string, onlyCache bool, proxies []string) (map[string]string, error) {
    if onlyCache {
        cacheKey := FormatCacheKey(KeyTypeRanking, config, group, "")
        cachedData, ok := GetCacheValue(cacheKey)
        if ok {
            if rankingData, isRanking := cachedData.(RankingData); isRanking && len(rankingData.Ranking) > 0 {
                return rankingData.Ranking, nil
            } else if rankingMap, isMap := cachedData.(map[string]string); isMap && len(rankingMap) > 0 {
                return rankingMap, nil
            }
        }
        
        dbKey := FormatDBKey("smart", KeyTypeRanking, config, group, "")
        data, err := s.DBViewGetItem(dbKey)
        if err == nil && data != nil {
            var rankingData RankingData
            if json.Unmarshal(data, &rankingData) == nil && len(rankingData.Ranking) > 0 {
                SetCacheValue(cacheKey, rankingData)
                return rankingData.Ranking, nil
            }
        }
        
        return make(map[string]string), nil
    }
    
    var allNodes []string
    if len(proxies) > 0 {
        allNodes = proxies
    } else {
        allNodes, _ = s.GetAllNodesForGroup(group, config)
    }
        
    nodeDataMap := make(map[string]*struct{
        tcpWeights    float64
        tcpSamples    int
        udpWeights    float64
        udpSamples    int
        asnSamples    int
        finalWeight   float64
        degradeFactor float64
    }, len(allNodes))
    
    nodeStatesMap := make(map[string]NodeState)
    stateData, _ := s.GetNodeStates(group, config)
    
    for nodeName, data := range stateData {
        var state NodeState
        if json.Unmarshal(data, &state) == nil {
            if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
                continue
            }
            
            nodeStatesMap[nodeName] = state
            
            nodeDataMap[nodeName] = &struct{
                tcpWeights    float64
                tcpSamples    int
                udpWeights    float64
                udpSamples    int
                asnSamples    int
                finalWeight   float64
                degradeFactor float64
            }{
                degradeFactor: 1.0,
            }
            
            if state.Degraded {
                nodeDataMap[nodeName].degradeFactor = state.DegradedFactor
            }
        }
    }
    
    for _, nodeName := range allNodes {
        if _, exists := nodeDataMap[nodeName]; !exists {
            nodeDataMap[nodeName] = &struct{
                tcpWeights    float64
                tcpSamples    int
                udpWeights    float64
                udpSamples    int
                asnSamples    int
                finalWeight   float64
                degradeFactor float64
            }{
                degradeFactor: 1.0,
            }
        }
    }
    
    now := time.Now().Unix()
    decayCache := make(map[int64]float64, 72)
    
    totalNodes := len(allNodes)
    minDecay := math.Max(0.1, 0.4 - float64(totalNodes)*0.005)
    
    getTimeDecay := func(lastUsedTime int64) float64 {
        return GetTimeDecayWithCache(lastUsedTime, now, minDecay, decayCache)
    }
    
    allStats, err := s.GetAllStats(group, config, true)
    if err != nil {
        return nil, err
    }
    
    for _, nodeStats := range allStats {
        for nodeName, data := range nodeStats {
            nodeData, ok := nodeDataMap[nodeName]
            if !ok {
                continue
            }
            
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }
            
            samples := record.Success + record.Failure
            if samples < DefaultMinSampleCount {
                continue
            }
            
            timeDecay := getTimeDecay(record.LastUsed.Unix())
            timeDecayedSamples := float64(samples) * timeDecay
            
            if record.Weights == nil {
                continue
            }
            
            // 处理TCP权重
            if tcpWeight, ok := record.Weights[WeightTypeTCP]; ok && tcpWeight > 0 {
                nodeData.tcpWeights += tcpWeight * timeDecay * float64(samples)
                nodeData.tcpSamples += int(timeDecayedSamples)
            }
            
            // 处理UDP权重
            if udpWeight, ok := record.Weights[WeightTypeUDP]; ok && udpWeight > 0 {
                nodeData.udpWeights += udpWeight * timeDecay * float64(samples)
                nodeData.udpSamples += int(timeDecayedSamples)
            }
            
            // 处理ASN权重 - 只统计数量，具体权重在第二次遍历中处理
            for key := range record.Weights {
                if strings.HasPrefix(key, WeightTypeTCPASN) || strings.HasPrefix(key, WeightTypeUDPASN) {
                    nodeData.asnSamples++
                }
            }
        }
    }
    
    for _, nodeStats := range allStats {
        for nodeName, data := range nodeStats {
            nodeData, ok := nodeDataMap[nodeName]
            if !ok || nodeData.asnSamples == 0 {
                continue
            }
            
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }
            
            timeDecay := getTimeDecay(record.LastUsed.Unix())
            
            // 处理ASN权重 - 现在已经确定此节点有ASN样本
            for key, weight := range record.Weights {
                if strings.HasPrefix(key, WeightTypeTCPASN) && weight > 0 {
                    // ASN权重贡献限制为25%
                    asnBonus := weight * timeDecay * 0.25
                    nodeData.tcpWeights += asnBonus
                    nodeData.tcpSamples++
                } else if strings.HasPrefix(key, WeightTypeUDPASN) && weight > 0 {
                    // ASN权重贡献限制为25%
                    asnBonus := weight * timeDecay * 0.25
                    nodeData.udpWeights += asnBonus
                    nodeData.udpSamples++
                }
            }
        }
    }
    
    nodeWeights := make(map[string]float64, len(nodeDataMap))
    
    for nodeName, data := range nodeDataMap {
        if data.tcpSamples > 0 {
            tcpAvgWeight := data.tcpWeights / float64(data.tcpSamples)
            tcpFinalWeight := tcpAvgWeight * data.degradeFactor
            data.finalWeight = tcpFinalWeight
        }
        
        if data.udpSamples > 0 {
            udpAvgWeight := data.udpWeights / float64(data.udpSamples)
            udpFinalWeight := udpAvgWeight * data.degradeFactor
            
            // 如果已经有TCP权重，则取平均值
            if data.finalWeight > 0 {
                data.finalWeight = (data.finalWeight + udpFinalWeight) / 2
            } else {
                data.finalWeight = udpFinalWeight
            }
        }
        
        if data.finalWeight > 0 {
            nodeWeights[nodeName] = data.finalWeight
        }
    }
    
    type nodeWeight struct {
        name   string
        weight float64
    }

    var nodesList []nodeWeight
    for name, weight := range nodeWeights {
        nodesList = append(nodesList, nodeWeight{name, weight})
    }
    
    sort.Slice(nodesList, func(i, j int) bool {
        return nodesList[i].weight > nodesList[j].weight
    })

    result := make(map[string]string)
    
    for _, node := range nodesList {
        result[node.name] = RankOccasional
    }

    if len(nodesList) > 0 {
        result[nodesList[0].name] = RankMostUsed
    
        if len(nodesList) == 2 {
            result[nodesList[1].name] = RankOccasional
        } else if len(nodesList) >= 3 {
            mostUsedBound := int(float64(len(nodesList)) * 0.2)
            if mostUsedBound < 1 {
                mostUsedBound = 1
            }
            
            occasionalBound := mostUsedBound + int(float64(len(nodesList)) * 0.5)
            
            for i := 1; i < mostUsedBound; i++ {
                result[nodesList[i].name] = RankMostUsed
            }
            
            for i := mostUsedBound; i < occasionalBound; i++ {
                result[nodesList[i].name] = RankOccasional
            }
            
            for i := occasionalBound; i < len(nodesList); i++ {
                result[nodesList[i].name] = RankRarelyUsed
            }
        }
    }
    
    if len(nodeWeights) > 0 {
        for _, nodeName := range allNodes {
            if _, exists := nodeWeights[nodeName]; !exists {
                result[nodeName] = RankRarelyUsed
            }
        }
    }

    s.StoreNodeWeightRanking(group, config, result)

    return result, nil
}

// 存储节点权重排名
func (s *Store) StoreNodeWeightRanking(group, config string, ranking map[string]string) error {
    rankingData := RankingData{
        Ranking:     ranking,
        LastUpdated: time.Now(),
    }
    
    cacheKey := FormatCacheKey(KeyTypeRanking, config, group, "")
    dbKey := FormatDBKey("smart", KeyTypeRanking, config, group, "")
    
    SetCacheValue(cacheKey, rankingData)
    
    data, err := json.Marshal(rankingData)
    if err != nil {
        return fmt.Errorf("failed to serialize ranking data: %w", err)
    }

    err = s.DBBatchPutItem(dbKey, data)

    return err
}

// 获取目标的最佳代理
func (s *Store) GetBestProxyForTarget(group, config string, target string, weightType string, allStats bool) (string, float64, map[string]float64, error) {
    if target == "" {
        return "", 0, nil, errors.New("empty target")
    }

    now := time.Now().Unix()
    minDecay := math.Max(0.1, 0.4)
    decayCache := make(map[int64]float64, 72)

    getTimeDecay := func(lastUsedTime int64) float64 {
        return GetTimeDecayWithCache(lastUsedTime, now, minDecay, decayCache)
    }

    allStatsMap, err := s.GetAllStats(group, config, allStats)
    if err != nil {
        return "", 0, nil, err
    }

    nodeStatesMap := make(map[string]NodeState)
    allAvailableNodes := make([]string, 0)
    stateData, _ := s.GetNodeStates(group, config)
    for nodeName, data := range stateData {
        var state NodeState
        if err := json.Unmarshal(data, &state); err == nil {
            if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
                continue
            }
            nodeStatesMap[nodeName] = state
            allAvailableNodes = append(allAvailableNodes, nodeName)
        }
    }
    availableNodesCount := len(allAvailableNodes)

    nodesWithWeight := make(map[string]float64)
    var domainStats map[string][]byte
    if stats, ok := allStatsMap[target]; ok {
        domainStats = stats
    } else {
        return "", 0, nodesWithWeight, nil
    }

    asnMode := strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN)
    nodeSamples := make(map[string]int)
    for nodeName, data := range domainStats {
        var record StatsRecord
        if json.Unmarshal(data, &record) != nil {
            continue
        }
        if asnMode {
            if record.Weights != nil {
                if weight, ok := record.Weights[weightType]; ok && weight > 0 {
                    timeDecay := getTimeDecay(record.LastUsed.Unix())
                    nodesWithWeight[nodeName] += weight * timeDecay
                    nodeSamples[nodeName]++
                }
            }
        } else {
            var weight float64
            if record.Weights != nil {
                weight = record.Weights[weightType]
            }
            if weight > 0 {
                timeDecay := getTimeDecay(record.LastUsed.Unix())
                decayedWeight := weight * timeDecay
                if state, exists := nodeStatesMap[nodeName]; exists && state.Degraded {
                    decayedWeight *= state.DegradedFactor
                }
                nodesWithWeight[nodeName] = decayedWeight
            }
        }
    }

    if asnMode {
        for nodeName, totalWeight := range nodesWithWeight {
            samples := nodeSamples[nodeName]
            if samples >= DefaultMinSampleCount {
                avgWeight := totalWeight / float64(samples)
                nodesWithWeight[nodeName] = avgWeight
            } else {
                delete(nodesWithWeight, nodeName)
            }
        }
        for nodeName, weight := range nodesWithWeight {
            if state, ok := nodeStatesMap[nodeName]; ok && state.Degraded {
                nodesWithWeight[nodeName] = weight * state.DegradedFactor
            }
        }
    }

    var requiredNodeCount int
    if asnMode {
        baseCount := func() int {
            switch {
            case availableNodesCount <= 5:
                return 1
            case availableNodesCount <= 10:
                return 2
            case availableNodesCount <= 20:
                return 3
            case availableNodesCount <= 50:
                return 4
            default:
                return 5
            }
        }()
        coverageRatio := 0.0
        if availableNodesCount > 0 {
            coverageRatio = float64(len(nodesWithWeight)) / float64(availableNodesCount)
        }
        switch {
        case coverageRatio >= 0.6:
            requiredNodeCount = baseCount + 1
        case coverageRatio >= 0.3:
            requiredNodeCount = baseCount
        case coverageRatio >= 0.1:
            requiredNodeCount = (baseCount * 2) / 3
            if requiredNodeCount < 1 {
                requiredNodeCount = 1
            }
        default:
            requiredNodeCount = 1
        }
        if len(nodesWithWeight) >= 3 {
            var maxWeight, minWeight float64
            first := true
            for _, weight := range nodesWithWeight {
                if first {
                    maxWeight = weight
                    minWeight = weight
                    first = false
                } else {
                    if weight > maxWeight {
                        maxWeight = weight
                    }
                    if weight < minWeight {
                        minWeight = weight
                    }
                }
            }
            if maxWeight > 0 && minWeight > 0 {
                ratio := maxWeight / minWeight
                switch {
                case ratio >= 4.0:
                    requiredNodeCount = (requiredNodeCount * 2) / 3
                    if requiredNodeCount < 1 {
                        requiredNodeCount = 1
                    }
                case ratio >= 2.0:
                    requiredNodeCount = (requiredNodeCount * 4) / 5
                    if requiredNodeCount < 1 {
                        requiredNodeCount = 1
                    }
                case ratio >= 1.5:
                    requiredNodeCount = requiredNodeCount
                case ratio < 1.3:
                    requiredNodeCount = requiredNodeCount + 1
                }
                if maxWeight < 0.8 {
                    requiredNodeCount = (requiredNodeCount * 3) / 4
                    if requiredNodeCount < 1 {
                        requiredNodeCount = 1
                    }
                }
                if maxWeight > 2.5 && ratio >= 1.8 {
                    requiredNodeCount = (requiredNodeCount * 3) / 4
                    if requiredNodeCount < 1 {
                        requiredNodeCount = 1
                    }
                }
            }
        }
        if requiredNodeCount > len(nodesWithWeight) {
            requiredNodeCount = len(nodesWithWeight)
        }
        if requiredNodeCount > availableNodesCount/2 {
            requiredNodeCount = availableNodesCount / 2
            if requiredNodeCount < 1 {
                requiredNodeCount = 1
            }
        }
    } else {
        switch {
        case availableNodesCount < 10:
            requiredNodeCount = availableNodesCount / 2
            if requiredNodeCount < 1 {
                requiredNodeCount = 1
            }
        case availableNodesCount < 30:
            requiredNodeCount = availableNodesCount / 4
        case availableNodesCount > 50 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.1:
            requiredNodeCount = 4
        case availableNodesCount > 100 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.05:
            requiredNodeCount = 2
        default:
            requiredNodeCount = 5
        }
    }

    if len(nodesWithWeight) >= requiredNodeCount && requiredNodeCount > 0 {
        var bestNode string
        var bestWeight float64
        for node, weight := range nodesWithWeight {
            if weight > bestWeight {
                bestWeight = weight
                bestNode = node
            }
        }
        return bestNode, bestWeight, nodesWithWeight, nil
    } else {
        return "", 0, nodesWithWeight, nil
    }
}

// 获取活跃域名
func (h domainMinHeap) Len() int           { return len(h) }
func (h domainMinHeap) Less(i, j int) bool { return h[i].lastUsed.Before(h[j].lastUsed) }
func (h domainMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *domainMinHeap) Push(x interface{}) { *h = append(*h, x.(domainLastUsed)) }
func (h *domainMinHeap) Pop() interface{} {
    old := *h
    n := len(old)
    x := old[n-1]
    *h = old[0 : n-1]
    return x
}

func (s *Store) GetActiveDomains(group, config string, limit int, all bool) (map[string][]string, error) {
    allStats, err := s.GetAllStats(group, config, all)
    if err != nil {
        return nil, err
    }
    if len(allStats) == 0 {
        return nil, nil
    }

    h := &domainMinHeap{}
    heap.Init(h)

    for domain, nodeStats := range allStats {
        var maxLastUsed time.Time
        activeTypeSet := make(map[string]struct{})
        for _, data := range nodeStats {
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }
            if maxLastUsed.IsZero() || record.LastUsed.After(maxLastUsed) {
                maxLastUsed = record.LastUsed
            }
            if record.Weights != nil {
                if w, ok := record.Weights[WeightTypeTCP]; ok && w > 0 {
                    activeTypeSet[WeightTypeTCP] = struct{}{}
                }
                if w, ok := record.Weights[WeightTypeUDP]; ok && w > 0 {
                    activeTypeSet[WeightTypeUDP] = struct{}{}
                }
            }
        }
        if maxLastUsed.IsZero() || len(activeTypeSet) == 0 {
            continue
        }
        types := make([]string, 0, len(activeTypeSet))
        for t := range activeTypeSet {
            types = append(types, t)
        }
        heap.Push(h, domainLastUsed{
            domain:   domain,
            lastUsed: maxLastUsed,
            types:    types,
        })
        if h.Len() > limit {
            heap.Pop(h)
        }
    }

    result := make(map[string][]string)
    var sorted []domainLastUsed
    for h.Len() > 0 {
        sorted = append(sorted, heap.Pop(h).(domainLastUsed))
    }
    for i := len(sorted) - 1; i >= 0; i-- {
        result[sorted[i].domain] = sorted[i].types
    }
    return result, nil
}


// 获取活跃的ASN
func (h asnMinHeap) Len() int           { return len(h) }
func (h asnMinHeap) Less(i, j int) bool { return h[i].lastUsed.Before(h[j].lastUsed) }
func (h asnMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *asnMinHeap) Push(x interface{}) { *h = append(*h, x.(asnLastUsed)) }
func (h *asnMinHeap) Pop() interface{} {
    old := *h
    n := len(old)
    x := old[n-1]
    *h = old[0 : n-1]
    return x
}

func (s *Store) GetActiveASNs(group, config string, limit int, all bool) map[string][]string {
    asnLastUsedMap := make(map[string]time.Time)
    asnTypeSet := make(map[string]map[string]struct{})
    asnFrequency := make(map[string]int)

    allStats, err := s.GetAllStats(group, config, all)
    if err != nil {
        return nil
    }

    for _, nodeStats := range allStats {
        for _, data := range nodeStats {
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }
            for weightType := range record.Weights {
                if strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN) {
                    parts := strings.Split(weightType, ":")
                    if len(parts) >= 2 {
                        asn := parts[1]
                        asnFrequency[asn]++
                        if lastUsed, exists := asnLastUsedMap[asn]; !exists || record.LastUsed.After(lastUsed) {
                            asnLastUsedMap[asn] = record.LastUsed
                        }
                        if asnTypeSet[asn] == nil {
                            asnTypeSet[asn] = make(map[string]struct{})
                        }
                        if strings.HasPrefix(weightType, WeightTypeTCPASN) {
                            asnTypeSet[asn][WeightTypeTCP] = struct{}{}
                        }
                        if strings.HasPrefix(weightType, WeightTypeUDPASN) {
                            asnTypeSet[asn][WeightTypeUDP] = struct{}{}
                        }
                    }
                }
            }
        }
    }

    h := &asnMinHeap{}
    heap.Init(h)
    for asn, lastUsed := range asnLastUsedMap {
        if asnFrequency[asn] < DefaultMinSampleCount {
            continue
        }
        typeSet := asnTypeSet[asn]
        types := make([]string, 0, len(typeSet))
        for t := range typeSet {
            types = append(types, t)
        }
        heap.Push(h, asnLastUsed{
            asn:      asn,
            lastUsed: lastUsed,
            types:    types,
        })
        if h.Len() > limit {
            heap.Pop(h)
        }
    }

    result := make(map[string][]string)
    var sorted []asnLastUsed
    for h.Len() > 0 {
        sorted = append(sorted, heap.Pop(h).(asnLastUsed))
    }
    for i := len(sorted) - 1; i >= 0; i-- {
        result[sorted[i].asn] = sorted[i].types
    }
    return result
}

// RunPrefetch 最佳节点预先获取
func (s *Store) RunPrefetch(group, config string, proxyMap map[string]string) int {
    log.Debugln("[SmartStore] Executing domain and ASN pre-calculation for policy group [%s]", group)

    blockedNodes := make(map[string]bool)
    stateData, _ := s.GetNodeStates(group, config)
    for nodeName, data := range stateData {
        var state NodeState
        if json.Unmarshal(data, &state) == nil {
            if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
                blockedNodes[nodeName] = true
            }
        }
    }

    availableProxyMap := make(map[string]string)
    for name, value := range proxyMap {
        if !blockedNodes[name] {
            availableProxyMap[name] = value
        }
    }

    if len(availableProxyMap) == 0 {
        log.Debugln("[SmartStore] No available nodes for prefetch calculation in group [%s]", group)
        return 0
    }

    globalCacheParams.mutex.RLock()
    prefetchLimit := globalCacheParams.PrefetchLimit
    globalCacheParams.mutex.RUnlock()

    if prefetchLimit <= 0 {
        prefetchLimit = MinPrefetchDomainsLimit
    }

    domains, _ := s.GetActiveDomains(group, config, prefetchLimit, true)
    asns := s.GetActiveASNs(group, config, prefetchLimit/2, true)

    prefetchCount := 0

    type prefetchItem struct {
        target     string
        weightType string
        bestNode   string
        bestWeight float64
    }

    var domainItems []prefetchItem
    var asnItems []prefetchItem

    // 域名
    for domain, activeTypes := range domains {
        for _, weightType := range activeTypes {
            bestNode, bestWeight, _, err := s.GetBestProxyForTarget(group, config, domain, weightType, true)
            if err != nil || bestNode == "" || bestWeight <= 0 {
                continue
            }
            if _, exists := availableProxyMap[bestNode]; exists {
                item := prefetchItem{
                    target:     domain,
                    weightType: weightType,
                    bestNode:   bestNode,
                    bestWeight: bestWeight,
                }
                domainItems = append(domainItems, item)
            }
        }
    }

    // ASN
    for asn, activeTypes := range asns {
        for _, baseType := range activeTypes {
            var weightType string
            if baseType == WeightTypeTCP {
                weightType = WeightTypeTCPASN + ":" + asn
            } else if baseType == WeightTypeUDP {
                weightType = WeightTypeUDPASN + ":" + asn
            } else {
                continue
            }
            bestNode, bestWeight, _, err := s.GetBestProxyForTarget(group, config, asn, weightType, true)
            if err != nil || bestNode == "" || bestWeight <= 0 {
                continue
            }
            if _, exists := availableProxyMap[bestNode]; exists {
                item := prefetchItem{
                    target:     asn,
                    weightType: weightType,
                    bestNode:   bestNode,
                    bestWeight: bestWeight,
                }
                asnItems = append(asnItems, item)
            }
        }
    }

    // 域名
    for _, item := range domainItems {
        oldNode, oldWeight := s.GetPrefetchResult(group, config, item.target, item.weightType)
        if item.bestWeight == 0 {
            continue
        }
        if oldNode == "" {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
            prefetchCount++
            log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (no old result)",
                item.target, item.bestNode, group, item.weightType, item.bestWeight)
            continue
        }
        if oldNode == item.bestNode {
            if item.bestWeight != oldWeight && item.bestWeight > 0 {
                s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
                prefetchCount++
                log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, same node, weight changed)",
                    item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
            }
        } else if item.bestWeight > oldWeight {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
            prefetchCount++
            log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, upgraded)",
                item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
        }
    }

    // ASN
    for _, item := range asnItems {
        oldNode, oldWeight := s.GetPrefetchResult(group, config, item.target, item.weightType)
        if item.bestWeight == 0 {
            continue
        }
        if oldNode == "" {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
            prefetchCount++
            log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (no old result)",
                item.target, item.bestNode, group, item.weightType, item.bestWeight)
            continue
        }
        if oldNode == item.bestNode {
            if item.bestWeight != oldWeight && item.bestWeight > 0 {
                s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
                prefetchCount++
                log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, same node, weight changed)",
                    item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
            }
        } else if item.bestWeight > oldWeight {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
            prefetchCount++
            log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, upgraded)",
                item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
        }
    }

    log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated %d domain/ASN mappings",
        group, prefetchCount)
    return prefetchCount
}

// GetNodeStates 获取节点状态
func (s *Store) GetNodeStates(group, config string) (map[string][]byte, error) {
    pathPrefix := FormatDBKey("smart", KeyTypeNode, config, group, "")
    
    cacheKeyPrefix := FormatCacheKey(KeyTypeNode, config, group, "")
    cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)
    
    if len(cacheResults) > 0 {
        result := make(map[string][]byte, len(cacheResults))
        allFromCache := true
        
        for key, value := range cacheResults {
            parts := strings.Split(key, ":")
            if len(parts) > 0 {
                nodeName := parts[len(parts)-1]
                var data []byte
                var err error
                
                switch v := value.(type) {
                case []byte:
                    data = make([]byte, len(v))
                    copy(data, v)
                case NodeState:
                    data, err = json.Marshal(v)
                default:
                    allFromCache = false
                    continue
                }
                
                if err == nil && data != nil {
                    result[nodeName] = data
                } else {
                    allFromCache = false
                }
            }
        }
        
        if allFromCache {
            globalQueueMutex.Lock()
            for _, op := range globalOperationQueue {
                if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
                    result[op.Node] = op.Data
                }
            }
            globalQueueMutex.Unlock()
            
            return result, nil
        }
    }
    
    rawResult, err := s.GetSubBytesByPath(pathPrefix, true)
    if err != nil {
        return nil, err
    }
    
    result := make(map[string][]byte)
    
    for fullPath, data := range rawResult {
        parts := strings.Split(fullPath, "/")
        if len(parts) > 0 {
            nodeName := parts[len(parts)-1]
            result[nodeName] = data
        }
    }
    
    for nodeName, data := range result {
        cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
        var nodeState NodeState
        if json.Unmarshal(data, &nodeState) == nil {
            SetCacheValue(cacheKey, nodeState)
        } else {
            SetCacheValue(cacheKey, data)
        }
    }
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
            result[op.Node] = op.Data
        }
    }
    globalQueueMutex.Unlock()
    
    return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForDomain(group, config, domain string) (map[string][]byte, error) {
    cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, domain)
    
    cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)
    
    if len(cacheResults) > 0 {
        result := make(map[string][]byte, len(cacheResults))
        allFromCache := true
        
        for key, value := range cacheResults {
            parts := strings.Split(key, ":")
            if len(parts) >= 5 {
                nodeName := parts[len(parts)-1]
                var data []byte
                var err error
                
                switch v := value.(type) {
                case []byte:
                    data = make([]byte, len(v))
                    copy(data, v)
                case StatsRecord:
                    data, err = json.Marshal(v)
                default:
                    allFromCache = false
                    continue
                }
                
                if err == nil && data != nil {
                    result[nodeName] = data
                } else {
                    allFromCache = false
                }
            }
        }
        
        if allFromCache && len(result) > 0 {
            globalQueueMutex.Lock()
            for _, op := range globalOperationQueue {
                if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
                    result[op.Node] = op.Data
                }
            }
            globalQueueMutex.Unlock()
            
            return result, nil
        }
    }
    
    pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, domain, "")
    rawResult, err := s.GetSubBytesByPath(pathPrefix, false)
    if err != nil {
        return nil, err
    }

    result := make(map[string][]byte)

    for fullPath, data := range rawResult {
        parts := strings.Split(fullPath, "/")
        if len(parts) > 0 {
            nodeName := parts[len(parts)-1]  
            result[nodeName] = data
            
            cacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, nodeName)
            var record StatsRecord
            if json.Unmarshal(data, &record) == nil {
                SetCacheValue(cacheKey, record)
            } else {
                SetCacheValue(cacheKey, data)
            }
        }
    }
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
            result[op.Node] = op.Data
        }
    }
    globalQueueMutex.Unlock()
    
    return result, nil
}

// 获取所有统计数据
func (s *Store) GetAllStats(group, config string, all bool) (map[string]map[string][]byte, error) {
    cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, "")
    cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)

    globalCacheParams.mutex.RLock()
    configMaxDomains := globalCacheParams.MaxDomains
    globalCacheParams.mutex.RUnlock()

    maxDomainsLimit := 1000
    if all {
        maxDomainsLimit = configMaxDomains
    } else if configMaxDomains < 1000 {
        maxDomainsLimit = configMaxDomains
    }

    result := make(map[string]map[string][]byte)
    domainsCount := 0

    for key, value := range cacheResults {
        if domainsCount >= maxDomainsLimit {
            break
        }
        parts := strings.Split(key, ":")
        if len(parts) >= 5 {
            domain := parts[len(parts)-2]
            nodeName := parts[len(parts)-1]
            if _, exists := result[domain]; !exists {
                if domainsCount >= maxDomainsLimit {
                    break
                }
                result[domain] = make(map[string][]byte)
                domainsCount++
            }
            var data []byte
            var err error
            switch v := value.(type) {
            case []byte:
                data = make([]byte, len(v))
                copy(data, v)
            case StatsRecord:
                data, err = json.Marshal(v)
            default:
                continue
            }
            if err == nil && data != nil {
                result[domain][nodeName] = data
            }
        }
    }

    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveStats && op.Group == group && op.Config == config {
            domain := op.Domain
            nodeName := op.Node
            if _, exists := result[domain]; !exists {
                if domainsCount >= maxDomainsLimit {
                    continue
                }
                result[domain] = make(map[string][]byte)
                domainsCount++
            }
            result[domain][nodeName] = op.Data
        }
    }
    globalQueueMutex.Unlock()

    if len(result) < maxDomainsLimit {
        pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")
        rawResult, err := s.DBViewPrefixScan(pathPrefix, maxDomainsLimit)
        if err != nil {
            return nil, err
        }
        for path, data := range rawResult {
            if len(result) >= maxDomainsLimit {
                break
            }
            parts := strings.Split(path, "/")
            if len(parts) < 6 {
                continue
            }
            domain := parts[len(parts)-2]
            node := parts[len(parts)-1]
            if _, exists := result[domain]; !exists {
                result[domain] = make(map[string][]byte)
            }
            if _, exists := result[domain][node]; !exists {
                result[domain][node] = data
                cacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, node)
                var record StatsRecord
                if json.Unmarshal(data, &record) == nil {
                    SetCacheValue(cacheKey, record)
                } else {
                    SetCacheValue(cacheKey, data)
                }
            }
        }
    }

    return result, nil
}

// 获取所有域名记录
func (s *Store) GetAllDomainRecords(group, config string) ([]DomainRecord, error) {
    allStats, err := s.GetAllStats(group, config, true)
    if err != nil {
        return nil, err
    }

    var records []DomainRecord
    for domain, nodeStats := range allStats {
        for nodeName, data := range nodeStats {
            var statsRecord StatsRecord
            if err := json.Unmarshal(data, &statsRecord); err != nil {
                continue
            }

            records = append(records, DomainRecord{
                Key:      fmt.Sprintf("%s:%s:%s:%s", config, group, nodeName, domain),
                Domain:   domain,
                NodeName: nodeName,
                LastUsed: statsRecord.LastUsed,
            })
        }
    }

    sort.Slice(records, func(i, j int) bool {
        return records[i].LastUsed.After(records[j].LastUsed)
    })

    return records, nil
}

// 删除域名记录
func (s *Store) DeleteDomainRecords(group, config, domain string) error {
    key := FormatDBKey("smart", KeyTypeStats, config, group, domain, "")
    return s.DeleteByPath(key)
}

// 获取配置中的所有组
func (s *Store) GetAllGroupsForConfig(config string) ([]string, error) {
    groupsMap := make(map[string]bool)
    
    statsPath := FormatDBKey("smart", KeyTypeStats, config)
    prefix := statsPath + "/"
    
    scanResults, err := s.DBViewPrefixScan(prefix, 1000)
    if err != nil {
        return nil, err
    }
    
    for path := range scanResults {
        parts := strings.Split(path, "/")
        if len(parts) >= 4 {
            group := parts[3]
            groupsMap[group] = true
        }
    }
    
    result := make([]string, 0, len(groupsMap))
    for group := range groupsMap {
        result = append(result, group)
    }
    
    if len(result) == 0 {
        return []string{}, nil
    }

    return result, nil
}

// 通过缓存数据获取组中的节点
func (s *Store) GetAllNodesForGroup(group, config string) ([]string, error) {
    nodesMap := make(map[string]bool)
    nodesPath := FormatDBKey("smart", KeyTypeNode, config, group, "")
    nodeStatesData, err := s.GetSubBytesByPath(nodesPath, true)
    if err == nil {
        for key := range nodeStatesData {
            parts := strings.Split(key, "/")
            if len(parts) >= 5 {
                nodeName := parts[4]
                nodesMap[nodeName] = true
            }
        }
    }
    
    allStats, err := s.GetAllStats(group, config, true)
    if err == nil {
        for _, domainStats := range allStats {
            for nodeName := range domainStats {
                nodesMap[nodeName] = true
            }
        }
    }
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if (op.Group == group && op.Config == config) {
            if op.Type == OpSaveNodeState {
                nodesMap[op.Node] = true
            } else if op.Type == OpSaveStats {
                nodesMap[op.Node] = true
            }
        }
    }
    globalQueueMutex.Unlock()
    
    var result []string
    for node := range nodesMap {
        result = append(result, node)
    }
    
    return result, nil
}

// 移除节点数据
func (s *Store) RemoveNodesData(group, config string, nodes []string) error {
    if len(nodes) == 0 {
        return nil
    }
    
    globalQueueMutex.Lock()
    newQueue := make([]StoreOperation, 0, len(globalOperationQueue))
    for _, op := range globalOperationQueue {
        if op.Group == group && op.Config == config {
            nodeMatches := false
            for _, node := range nodes {
                if op.Node == node {
                    nodeMatches = true
                    break
                }
            }
            if !nodeMatches {
                newQueue = append(newQueue, op)
            }
        } else {
            newQueue = append(newQueue, op)
        }
    }
    globalOperationQueue = newQueue
    globalQueueMutex.Unlock()
    
    allStats, err := s.GetAllStats(group, config, true)
    if err != nil {
        return err
    }
    
    domainNodePairs := make(map[string][]string)
    for domain, nodeStats := range allStats {
        for _, nodeName := range nodes {
            if _, exists := nodeStats[nodeName]; exists {
                domainNodePairs[domain] = append(domainNodePairs[domain], nodeName)
            }
        }
    }
    
    for _, nodeName := range nodes {
        nodePath := FormatDBKey("smart", KeyTypeNode, config, group, nodeName)
        if err := s.DeleteByPath(nodePath); err != nil {
            log.Warnln("[SmartStore] Failed to delete node state for [%s]: %v", nodeName, err)
        }
        
        cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
        DeleteCacheValue(cacheKey)
    }
    
    for domain, nodeNames := range domainNodePairs {
        for _, nodeName := range nodeNames {
            statsCacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, nodeName)
            DeleteCacheValue(statsCacheKey)
            
            statsPath := FormatDBKey("smart", KeyTypeStats, config, group, domain, nodeName)
            if err := s.DeleteByPath(statsPath); err != nil {
                log.Warnln("[SmartStore] Failed to delete stats for [%s], domain [%s]: %v", nodeName, domain, err)
            }
        }
    }

    return nil
}

// 标记连接失败
func (s *Store) MarkConnectionFailed(group, config, host string) {
    if s == nil {
        return
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    
    key := FormatCacheKey(KeyTypeFailed, config, group, host)
    SetCacheValue(key, time.Now())
    
    failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
    failedDomains := GetCacheValuesByPrefix(failedPrefix)
    failedCount := len(failedDomains)


    if failedCount >= NetworkFailureThreshold {
        s.failureStatusLock.Lock()
        wasFailure := s.networkFailureStatus[groupKey]
        s.networkFailureStatus[groupKey] = true
        s.successCount[groupKey] = 0
        if !wasFailure {
            log.Warnln("[SmartStore] Network failure detected for group [%s:%s] after [%d] consecutive failures",
                group, config, failedCount)
            s.lastNetworkFailure[groupKey] = time.Now()
        }
        s.failureStatusLock.Unlock()
    }
}

// 标记连接成功
func (s *Store) MarkConnectionSuccess(group, config string) {
    if s == nil {
        return
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    s.failureStatusLock.Lock()
    defer s.failureStatusLock.Unlock()

    if s.networkFailureStatus[groupKey] {
        if s.successCount == nil {
            s.successCount = make(map[string]int)
        }

        s.successCount[groupKey]++

        if s.successCount[groupKey] >= 3 || time.Since(s.lastNetworkFailure[groupKey]) > 30*time.Second {
            s.networkFailureStatus[groupKey] = false
            log.Infoln("[SmartStore] Network recovered for group [%s:%s] after %d successful connections",
                group, config, s.successCount[groupKey])
            s.successCount[groupKey] = 0
            
            failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
            RemoveCacheValuesByPrefix(failedPrefix)
        }
    }
}

// 检查网络故障状态
func (s *Store) CheckNetworkFailure(group, config string) bool {
    if s == nil {
        return false
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    s.failureStatusLock.RLock()
    defer s.failureStatusLock.RUnlock()

    return s.networkFailureStatus[groupKey]
}

// 清理旧的域名记录
func (s *Store) CleanupOldDomains(group, config string) error {
    domains := make(map[string]time.Time)
    domainRecords, err := s.GetAllDomainRecords(group, config)
    if err != nil {
        return err
    }
    for _, record := range domainRecords {
        if lastUsed, exists := domains[record.Domain]; !exists || record.LastUsed.After(lastUsed) {
            domains[record.Domain] = record.LastUsed
        }
    }

    type domainInfo struct {
        domain   string
        lastUsed time.Time
    }
    var domainList []domainInfo
    for domain, lastUsed := range domains {
        domainList = append(domainList, domainInfo{
            domain:   domain,
            lastUsed: lastUsed,
        })
    }
    sort.Slice(domainList, func(i, j int) bool {
        return domainList[i].lastUsed.Before(domainList[j].lastUsed)
    })

    globalCacheParams.mutex.RLock()
    maxDomains := globalCacheParams.MaxDomains
    globalCacheParams.mutex.RUnlock()
    if maxDomains <= 0 {
        maxDomains = MinDomainsLimit
    }

    if len(domainList) > maxDomains {
        toDelete := domainList[:len(domainList)-maxDomains]

        for _, info := range toDelete {
            // 删除域名统计数据（缓存和DB）
            err := s.DeleteDomainRecords(group, config, info.domain)
            if err != nil {
                log.Warnln("[SmartStore] Failed to delete domain [%s]: %v", info.domain, err)
            }
            // 同时清理预取结果（缓存和DB）
            s.DeleteCacheResult(KeyTypePrefetch, group, config, info.domain)
            prefetchDBKey := FormatDBKey("smart", KeyTypePrefetch, config, group, info.domain)
            _ = s.DeleteByPath(prefetchDBKey)
        }

        log.Debugln("[SmartStore] Cleaned up [%d] old domain records, keeping the latest [%d] (group %s)",
            len(toDelete), maxDomains, group)
    }

    RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))

    return nil
}

// 清理过期统计数据
func (s *Store) CleanupExpiredStats(group, config string) error {
    records, err := s.GetAllDomainRecords(group, config)
    if err != nil {
        return err
    }

    threshold := time.Now().Add(-RetentionPeriod)
    var expiredDomains []string

    domainLastUsed := make(map[string]time.Time)
    for _, record := range records {
        lastUsed, exists := domainLastUsed[record.Domain]
        if !exists || record.LastUsed.After(lastUsed) {
            domainLastUsed[record.Domain] = record.LastUsed
        }
    }

    for domain, lastUsed := range domainLastUsed {
        if lastUsed.Before(threshold) {
            expiredDomains = append(expiredDomains, domain)
            err := s.DeleteDomainRecords(group, config, domain)
            if err != nil {
                log.Warnln("[SmartStore] Failed to delete expired domain [%s]: %v", domain, err)
            }
        }
    }

    if len(expiredDomains) > 0 {
        log.Debugln("[SmartStore] Deleted [%d] expired domains for group [%s]", len(expiredDomains), group)
    }

    return nil
}