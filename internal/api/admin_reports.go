// Copy as internal/api/admin_reports.go into central-sms-platform.
// Before use, add AdminInternalToken to the existing API Server and populate it
// from CENTRAL_SMS_ADMIN_INTERNAL_TOKEN (or your config struct). Keep these routes
// reachable only on a private network. They are for a trusted Portal/BFF and Postman.
package api

import (
    "context"
    "crypto/subtle"
    "net/http"
    "strconv"
    "strings"
    "time"

    "github.com/google/uuid"
)

// Add this field to your existing Server struct:
// AdminInternalToken string

func (s *Server) requireAdminReportInternal(w http.ResponseWriter, r *http.Request) bool {
    got := r.Header.Get("X-Internal-Service-Token")
    if got == "" || s.AdminInternalToken == "" || len(got) != len(s.AdminInternalToken) ||
        subtle.ConstantTimeCompare([]byte(got), []byte(s.AdminInternalToken)) != 1 {
        writeJSON(w, http.StatusUnauthorized, map[string]string{"error":"unauthorized_internal_service"})
        return false
    }
    return true
}

func reportLimit(r *http.Request) (int, bool) {
    limit:=50
    if raw:=r.URL.Query().Get("limit");raw!=""{n,err:=strconv.Atoi(raw);if err!=nil||n<1||n>100{return 0,false};limit=n}
    return limit,true
}
func reportTenantFilter(r *http.Request)(any,bool){raw:=strings.TrimSpace(r.URL.Query().Get("tenant_id"));if raw==""{return nil,true};id,err:=uuid.Parse(raw);if err!=nil{return nil,false};return id,true}
func reportDateRange(r *http.Request)(time.Time,time.Time,bool){to:=time.Now().UTC();from:=to.Add(-24*time.Hour);if raw:=r.URL.Query().Get("from");raw!=""{v,e:=time.Parse(time.RFC3339,raw);if e!=nil{return time.Time{},time.Time{},false};from=v.UTC()};if raw:=r.URL.Query().Get("to");raw!=""{v,e:=time.Parse(time.RFC3339,raw);if e!=nil{return time.Time{},time.Time{},false};to=v.UTC()};if !from.Before(to)||to.Sub(from)>366*24*time.Hour{return time.Time{},time.Time{},false};return from,to,true}

func (s *Server) adminListMessages(w http.ResponseWriter, r *http.Request) {
    if !s.requireAdminReportInternal(w,r){return}
    limit,ok:=reportLimit(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_limit"});return}
    tenantID,ok:=reportTenantFilter(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_tenant_id"});return}
    status:=strings.TrimSpace(r.URL.Query().Get("status"))
    allowed:=map[string]bool{"queued":true,"submitting":true,"sent":true,"pending":true,"delivered":true,"failed":true,"unknown":true}
    if status!=""&&!allowed[status]{writeJSON(w,400,map[string]string{"error":"invalid_status_filter"});return}
    from,to,ok:=reportDateRange(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_date_range"});return}
    ctx,cancel:=context.WithTimeout(r.Context(),5*time.Second);defer cancel()
    rows,err:=s.DB.Query(ctx,`
        SELECT id::text,COALESCE(tenant_id::text,''),COALESCE(campaign_id::text,''),
               recipient,sender_id,status,COALESCE(provider_message_id,''),
               COALESCE(external_message_id,''),COALESCE(error_code,''),created_at,updated_at
        FROM messages
        WHERE ($1::uuid IS NULL OR tenant_id=$1::uuid)
          AND (NULLIF($2,'') IS NULL OR status=$2)
          AND created_at >= $3 AND created_at < $4
        ORDER BY created_at DESC,id DESC LIMIT $5
    `,tenantID,status,from,to,limit)
    if err!=nil{writeJSON(w,500,map[string]string{"error":"message_report_failed"});return};defer rows.Close()
    type item struct{ID string `json:"id"`;TenantID string `json:"tenant_id"`;CampaignID string `json:"campaign_id,omitempty"`;Recipient string `json:"recipient"`;SenderID string `json:"sender_id"`;Status string `json:"status"`;ProviderMessageID string `json:"provider_message_id,omitempty"`;ExternalMessageID string `json:"external_message_id,omitempty"`;ErrorCode string `json:"error_code,omitempty"`;CreatedAt time.Time `json:"created_at"`;UpdatedAt time.Time `json:"updated_at"`}
    items:=make([]item,0,limit)
    for rows.Next(){var x item;if err:=rows.Scan(&x.ID,&x.TenantID,&x.CampaignID,&x.Recipient,&x.SenderID,&x.Status,&x.ProviderMessageID,&x.ExternalMessageID,&x.ErrorCode,&x.CreatedAt,&x.UpdatedAt);err!=nil{writeJSON(w,500,map[string]string{"error":"message_report_failed"});return};items=append(items,x)}
    if rows.Err()!=nil{writeJSON(w,500,map[string]string{"error":"message_report_failed"});return}
    writeJSON(w,200,map[string]any{"items":items,"limit":limit,"from":from,"to":to})
}

func (s *Server) adminListCampaigns(w http.ResponseWriter, r *http.Request) {
    if !s.requireAdminReportInternal(w,r){return}
    limit,ok:=reportLimit(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_limit"});return}
    tenantID,ok:=reportTenantFilter(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_tenant_id"});return}
    status:=strings.TrimSpace(r.URL.Query().Get("status"))
    if len(status)>30{writeJSON(w,400,map[string]string{"error":"invalid_status_filter"});return}
    from,to,ok:=reportDateRange(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_date_range"});return}
    ctx,cancel:=context.WithTimeout(r.Context(),5*time.Second);defer cancel()
    rows,err:=s.DB.Query(ctx,`
        SELECT c.id::text,COALESCE(c.tenant_id::text,''),c.sender_id,c.status,
               c.recipient_count,c.created_at,c.updated_at,c.scheduled_at,
               COUNT(m.id) FILTER (WHERE m.status='queued'),
               COUNT(m.id) FILTER (WHERE m.status='sent'),
               COUNT(m.id) FILTER (WHERE m.status='failed'),
               COUNT(m.id) FILTER (WHERE m.status='unknown')
        FROM campaigns c LEFT JOIN messages m ON m.campaign_id=c.id
        WHERE ($1::uuid IS NULL OR c.tenant_id=$1::uuid)
          AND (NULLIF($2,'') IS NULL OR c.status=$2)
          AND c.created_at >= $3 AND c.created_at < $4
        GROUP BY c.id ORDER BY c.created_at DESC,c.id DESC LIMIT $5
    `,tenantID,status,from,to,limit)
    if err!=nil{writeJSON(w,500,map[string]string{"error":"campaign_report_failed"});return};defer rows.Close()
    type item struct{ID string `json:"id"`;TenantID string `json:"tenant_id"`;SenderID string `json:"sender_id"`;Status string `json:"status"`;RecipientCount int `json:"recipient_count"`;CreatedAt time.Time `json:"created_at"`;UpdatedAt time.Time `json:"updated_at"`;ScheduledAt *time.Time `json:"scheduled_at,omitempty"`;Queued int64 `json:"queued"`;Sent int64 `json:"sent"`;Failed int64 `json:"failed"`;Unknown int64 `json:"unknown"`}
    items:=make([]item,0,limit)
    for rows.Next(){var x item;if err:=rows.Scan(&x.ID,&x.TenantID,&x.SenderID,&x.Status,&x.RecipientCount,&x.CreatedAt,&x.UpdatedAt,&x.ScheduledAt,&x.Queued,&x.Sent,&x.Failed,&x.Unknown);err!=nil{writeJSON(w,500,map[string]string{"error":"campaign_report_failed"});return};items=append(items,x)}
    if rows.Err()!=nil{writeJSON(w,500,map[string]string{"error":"campaign_report_failed"});return}
    writeJSON(w,200,map[string]any{"items":items,"limit":limit,"from":from,"to":to})
}

func (s *Server) adminMessagingSummary(w http.ResponseWriter, r *http.Request) {
    if !s.requireAdminReportInternal(w,r){return}
    from,to,ok:=reportDateRange(r);if !ok{writeJSON(w,400,map[string]string{"error":"invalid_date_range"});return}
    ctx,cancel:=context.WithTimeout(r.Context(),5*time.Second);defer cancel()
    var total,queued,submitting,sent,pending,delivered,failed,unknown int64
    err:=s.DB.QueryRow(ctx,`
        SELECT COUNT(*),COUNT(*) FILTER(WHERE status='queued'),COUNT(*) FILTER(WHERE status='submitting'),
               COUNT(*) FILTER(WHERE status='sent'),COUNT(*) FILTER(WHERE status='pending'),
               COUNT(*) FILTER(WHERE status='delivered'),COUNT(*) FILTER(WHERE status='failed'),
               COUNT(*) FILTER(WHERE status='unknown')
        FROM messages WHERE created_at >= $1 AND created_at < $2
    `,from,to).Scan(&total,&queued,&submitting,&sent,&pending,&delivered,&failed,&unknown)
    if err!=nil{writeJSON(w,500,map[string]string{"error":"messaging_summary_failed"});return}
    var campaigns int64
    err=s.DB.QueryRow(ctx,`SELECT COUNT(*) FROM campaigns WHERE created_at >= $1 AND created_at < $2`,from,to).Scan(&campaigns)
    if err!=nil{writeJSON(w,500,map[string]string{"error":"messaging_summary_failed"});return}
    writeJSON(w,200,map[string]any{"from":from,"to":to,"messages":map[string]int64{"total":total,"queued":queued,"submitting":submitting,"sent":sent,"pending":pending,"delivered":delivered,"failed":failed,"unknown":unknown},"campaigns_total":campaigns})
}

// Register these in the existing (*Server).Register method:
// mux.HandleFunc("GET /internal/v1/admin/messages", s.adminListMessages)
// mux.HandleFunc("GET /internal/v1/admin/campaigns", s.adminListCampaigns)
// mux.HandleFunc("GET /internal/v1/admin/dashboard/summary", s.adminMessagingSummary)
