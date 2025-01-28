package org.sunbird.insights.controller.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.*;
import org.sunbird.user.service.UserUtilityService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

import static org.sunbird.common.util.Constants.*;
@Service
public class InsightsServiceImpl implements InsightsService {

    private static final Logger log = LoggerFactory.getLogger(InsightsServiceImpl.class);
    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    private RedisCacheMgr redisCacheMgr;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    CbExtServerProperties extServerProperties;

    @Autowired
    UserUtilityService userUtilityService;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    ObjectMapper mapper = new ObjectMapper();

    public SBApiResponse insights(Map<String, Object> requestBody,String userId) throws Exception {
        String [] labelsCertificates = {extServerProperties.getInsightsLabelCertificatesAcross(),extServerProperties.getInsightsLabelCertificatesYourDepartment()} ;
        String [] labelsLearningHours = {extServerProperties.getInsightsLabelLearningHoursYourDepartment(),extServerProperties.getInsightsLabelLearningHoursAcross()} ;
        HashMap<String, Object> request = (HashMap<String, Object>) requestBody.get(REQUEST) == null ? new HashMap<>() : (HashMap<String, Object>) requestBody.get(REQUEST);
        HashMap<String, Object> filter = ((HashMap<String, Object>) request.get(FILTERS)) == null ? new HashMap<>() : ((HashMap<String, Object>) request.get(FILTERS));
        ArrayList<String> organizations = (ArrayList<String>) (filter.get(ORGANISATIONS)) ==null ? new ArrayList<>() : (ArrayList<String>) (filter.get(ORGANISATIONS));
        ArrayList<String> keys = nudgeKeys(organizations);
        String[] fieldsArray = keys.toArray(new String[keys.size()]);
        ArrayList<String> certificateOrgs= new ArrayList<>();
        certificateOrgs.add("across");
        ArrayList<String> certificate_keys = nudgeKeys(certificateOrgs);
        String[] fieldsArray_certificates = certificate_keys.toArray(new String[certificate_keys.size()]);
        ArrayList<Object> nudges = new ArrayList<>();
        List<String> lhpLearningHours =  redisCacheMgr.hget(INSIGHTS_LEARNING_HOURS_REDIS_KEY, serverProperties.getRedisInsightIndex(),fieldsArray);
        List<String> lhpCertifications = redisCacheMgr.hget(INSIGHTS_CERTIFICATIONS_REDIS_KEY, serverProperties.getRedisInsightIndex(),fieldsArray_certificates);
        if(lhpLearningHours == null)
            lhpLearningHours = new ArrayList<>();
        if(lhpCertifications ==null)
            lhpCertifications = new ArrayList<>();
        populateIfNudgeExist(lhpLearningHours, nudges, INSIGHTS_TYPE_LEARNING_HOURS,organizations,labelsLearningHours);
        populateIfNudgeExist(lhpCertifications, nudges, INSIGHTS_TYPE_CERTIFICATE,Arrays.asList(fieldsArray_certificates),labelsCertificates);
        HashMap<String, Object> responseMap = new HashMap<>();
        responseMap.put(WEEKLY_CLAPS, populateIfClapsExist(userId) );
        responseMap.put(NUDGES, nudges);
        SBApiResponse response = ProjectUtil.createDefaultResponse(API_USER_INSIGHTS);
        response.getResult().put(RESPONSE, responseMap);
        return response;
    }
    private ArrayList<String> nudgeKeys(ArrayList<String> organizations ){
        ArrayList<String> keys = new ArrayList<>();
        for (String org : organizations) {
           // keys.add(org + COLON + LABEL);
            keys.add(org + COLON + YESTERDAY);
            keys.add(org + COLON + TODAY);
        }
        return  keys;
    }
    private Map<String, Object> populateIfClapsExist(String userId) {
        Map<String, Object> userRequest = new HashMap<>();
        userRequest.put(LEARNER_STATUS_USER_ID, userId);
        List<String> fields = new ArrayList<>();
        fields.add(LEARNER_STATUS_USER_ID);
        fields.add(TOTAL_CLAPS);
        fields.add(W1);
        fields.add(W2);
        fields.add(W3);
        fields.add(W4);
        List<Map<String, Object>>  result=  cassandraOperation.getRecordsByProperties(KEYSPACE_SUNBIRD,
                LEARNER_STATS, userRequest, fields);
        LocalDate[]  dates = populateDate();
        if (result ==null || result.size() < 1) {
            result = new ArrayList<>();
            HashMap m = new HashMap();
            result.add(m);
        }

            result.get(0).put("startDate", dates[0]);
            result.get(0).put("endDate", dates[1]);
            return result.get(0);


    }
    public void populateIfNudgeExist(List<String> data, ArrayList<Object> nudges, String type, List<String> organizations,String labels[]) {
        for (int i = 0, j = 0; i < data.size(); i += 2, j++) {
           // String label = data.get(i);
            double yesterday = StringUtils.isNotBlank(data.get(i)) ? Double.parseDouble(data.get(i)) : 0.0;
            double today = StringUtils.isNotBlank(data.get(i+1)) ? Double.parseDouble(data.get(i+1)) : 0.0;
            double change;
            if (yesterday != 0.0 || today != 0.0) {
                if (yesterday != 0.0) {
                    change = ((today - yesterday) / Math.abs(yesterday)) * 100;
                } else {
                    change = 100.0;
                }
            } else {
                change = 0.0;
            }
            HashMap<String, Object> nudge = new HashMap<>();
            nudge.put(PROGRESS, roundToTwoDecimals(change));
            nudge.put(GROWTH, change > 0 ? POSITIVE : NEGATIVE);

            if(organizations.size() > j) {
                String replacer = "";
                if(type.equals(INSIGHTS_TYPE_CERTIFICATE))
                    replacer = String.valueOf ((int)Math.round(today));
                else
                    replacer =  String.valueOf (roundToTwoDecimals(today));

                nudge.put(LABEL, organizations.get(j).equals("across") ? labels[1].replace("{0}",replacer) : labels[0].replace("{0}", replacer));
                nudge.put(ORG, organizations.get(j));
            }
            nudge.put(TYPE, type);
            nudges.add(nudge);
        }
    }

    public LocalDate[] populateDate(){
        LocalDate currentDate = LocalDate.now();
        LocalDate endDate = currentDate.with(TemporalAdjusters.next(DayOfWeek.SUNDAY));
        LocalDate startDate = endDate.minusWeeks(4).plusDays(1);
        LocalDate[] local = new LocalDate[2];
        local[0] = startDate;
        local[1] = endDate;
        return local;
    }

    public static double roundToTwoDecimals(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    public SBApiResponse readInsightsForOrganisation(Map<String, Object> requestBody, String userId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_MICRO_SITE_INSIGHTS);
        try {
            Map<String, Object> request = (Map<String, Object>) requestBody.get(REQUEST);
            if (MapUtils.isEmpty(request)) {
                response.getParams().setStatus(Constants.FAILED);
                response.put(MESSAGE, "Request is Missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> filter = ((Map<String, Object>) request.get(FILTERS));
            if (MapUtils.isEmpty(filter)) {
                response.getParams().setStatus(Constants.FAILED);
                response.put(MESSAGE, "Filter is Missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            List<String> organizations = (ArrayList<String>) (filter.get(ORGANISATIONS));
            if (CollectionUtils.isEmpty(organizations)) {
                response.getParams().setStatus(Constants.FAILED);
                response.put(MESSAGE, "Organization is Required");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, String> insightKeyMapping = serverProperties.getInsightsMappingKey();
            String valueForInsightKey = insightKeyMapping.getOrDefault(filter.get(Constants.REQUEST_TYPE), Constants.ORGANISATION);
            Map<String, Object> responseMap = new HashMap<>();
            String organisationInsideFieldsProperty =  PropertiesCache.getInstance().getProperty(valueForInsightKey + Constants.INSIGHT_FIELD_KEY);
            String redisKeyForInsightsProperty =  PropertiesCache.getInstance().getProperty(valueForInsightKey + Constants.INSIGHT_REDIS_KEY_MAPPING);
            String cssPropertiesForInsightsProperty =  PropertiesCache.getInstance().getProperty(valueForInsightKey + Constants.INSIGHT_PROPERTY_FIELDS);
            if (StringUtils.isBlank(organisationInsideFieldsProperty) || StringUtils.isBlank(redisKeyForInsightsProperty)) {
                response.getParams().setStatus(Constants.FAILED);
                response.put(MESSAGE, "Not able to find value for requestType.");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, String> organisationInsideFields = mapper.readValue(organisationInsideFieldsProperty, new TypeReference<LinkedHashMap<String, Object>>() {
            });
            Map<String, String> redisKeyForInsight =  mapper.readValue(redisKeyForInsightsProperty, new TypeReference<LinkedHashMap<String, Object>>() {
            });
            Map<String, String> cssPropertiesForInsight = mapper.readValue(cssPropertiesForInsightsProperty, new TypeReference<LinkedHashMap<String, Object>>() {
            });
            List<Map<String, Object>> organisationDataMapList = new ArrayList<>();
            for (String organisationId: organizations) {
                Map<String, Object> organisationMap = new HashMap<>();
                List<Map<String, Object>> nudgesDataList = new ArrayList<>();
                for (Map.Entry<String,String> insightFields: organisationInsideFields.entrySet()) {
                    Map<String, Object> nudgesData = new HashMap<>();
                    nudgesData.put(Constants.ICON, insightFields.getValue());
                    populateNudgeForMicroSite(insightFields.getKey(), organisationId, cssPropertiesForInsight,
                            redisKeyForInsight.get(insightFields.getKey()), nudgesData);
                    nudgesDataList.add(nudgesData);
                }
                organisationMap.put(Constants.ORG_ID, organisationId);
                organisationMap.put(Constants.DATA, nudgesDataList);
                organisationDataMapList.add(organisationMap);
            }
            responseMap.put(NUDGES, organisationDataMapList);
            response.getResult().put(RESPONSE, responseMap);
        } catch (Exception e) {
            log.error("Failed to get Insight Info for OrgId", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public void populateNudgeForMicroSite(String label, String organisationId, Map<String, String> additionalCssFields, String redisKey, Map<String, Object> nudgesData) {
       List<String> redisData = redisCacheMgr.hget(redisKey, serverProperties.getRedisInsightIndex(), organisationId);
       if (CollectionUtils.isNotEmpty(redisData)) {
           nudgesData.put(Constants.LABEL, label);
           nudgesData.put(Constants.VALUE, redisData.get(0));
           nudgesData.putAll(additionalCssFields);
       } else {
           log.error("Not able to fetch Data from redis key for key: {} for organisation: {}", redisKey, organisationId);
       }
    }

    @Override
    public SBApiResponse fetchNationalLearningData() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_NATIONAL_LEARNING_WEEK_INSIGHTS);
        try {
            String redisKeyMappingStr = serverProperties.getNationalLearningInsightsRedisKeyMapping();
            String fieldIconsStr = serverProperties.getNationalLearningInsightsFields();
            String uiPropertiesStr = serverProperties.getNationalLearningInsightsPropertyFields();

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> redisKeyMapping = objectMapper.readValue(redisKeyMappingStr, new TypeReference<Map<String, String>>() {
            });
            Map<String, String> fieldIcons = objectMapper.readValue(fieldIconsStr, new TypeReference<Map<String, String>>() {
            });
            Map<String, Object> uiProperties = objectMapper.readValue(uiPropertiesStr, new TypeReference<Map<String, Object>>() {
            });

            List<Map<String, Object>> insightsList = new ArrayList<>();
            for (Map.Entry<String, String> entry : redisKeyMapping.entrySet()) {
                String insightLabel = entry.getKey();
                String redisKey = entry.getValue();
                String redisData = redisCacheMgr.getCacheFromDataRedish(redisKey, serverProperties.getRedisInsightIndex());
                Map<String, Object> insightData = new HashMap<>();
                String iconUrl = fieldIcons.get(insightLabel);
                insightData.put(ICON, iconUrl);
                insightData.put(LABEL, insightLabel);
                insightData.put(VALUE, redisData);
                insightData.putAll(uiProperties);
                insightsList.add(insightData);
            }
            response.getResult().put(DATA, insightsList);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            log.info("successfully fetching data from redis for National Learning week Insights");
        } catch (Exception e) {
            log.error("Error while fetching National Learning Week Insights", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse getCourseRecommendationsByDesignation(String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COURSE_RECOMMENDATIONS);
        String orgId = null;
        String designation = null;
        try {
            String userId = fetchUserIdFromToken(authToken, response);
            if (userId == null) return response;
            Map<String, Object> userData = userUtilityService.getUsersReadData(userId, null, null);

            if (CollectionUtils.isNotEmpty(Collections.singleton(userData))) {
                designation = extractDesignation(userData,response);
                if (StringUtils.isEmpty(designation)) {
                    log.warn("Designation is missing for user with ID: {}", userId);
                    return response;
                }
                designation = designation.toUpperCase();
                orgId = (String) userData.get(ROOT_ORG_ID);
            }
            String errMsg = validateUserInfo(orgId, response, designation);
            if (StringUtils.isNotEmpty(errMsg)) return response;

            String subKey = orgId + "_" + designation;
            String redisKey = serverProperties.getCourseRecommendationsByDesignationKey();
            List<String> redisData = redisCacheMgr.hget(redisKey, serverProperties.getRedisInsightIndex(), subKey);
            if (CollectionUtils.isEmpty(redisData) || StringUtils.isBlank(redisData.get(0))) {
                log.info("No recommendations courses found with this subKey{}",subKey);
                response.getParams().setStatus(Constants.FAILED);
                response.put(Constants.MESSAGE, "No recommendations courses found");
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            String recommendationsString = redisData.get(0);
            List<String> recommendations = Arrays.asList(recommendationsString.split(","));
            response.getParams().setStatus(Constants.SUCCESS);
            response.put(Constants.COURSE_LIST, recommendations);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            log.error("Error while fetching recommendation course", e);
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.MESSAGE, "Internal Server Error");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private String validateUserInfo(String orgId, SBApiResponse response, String designation) {
        if (StringUtils.isEmpty(orgId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.MESSAGE, "OrgId is Missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "OrgId is Missing";
        }
        if (StringUtils.isEmpty(designation)) {
            response.getParams().setStatus(Constants.FAILED);
            response.put(Constants.MESSAGE, "Designation is Missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Designation is Missing";
        }
        return null;
    }

    private String extractDesignation(Map<String, Object> userData, SBApiResponse response) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode userDataNode = objectMapper.convertValue(userData, JsonNode.class);
        JsonNode profileDetails = userDataNode.get(Constants.PROFILE_DETAILS);
        if (profileDetails == null) {
            log.warn("User profile details are missing in the provided user data.");
            response.getParams().setStatus(FAILED);
            response.put(MESSAGE, "User profile details are missing.");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        JsonNode professionalDetailsArray = profileDetails.get("professionalDetails");
        if (professionalDetailsArray == null || professionalDetailsArray.size() == 0) {
            log.warn("Professional details are missing or not an array in the user profile.");
            response.getParams().setStatus(FAILED);
            response.put(MESSAGE, "User professional details are not available");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        JsonNode professionalDetails = professionalDetailsArray.get(0);
      
        if (professionalDetails == null) {
            log.warn("Professional details are missing in the user profile.");
            response.getParams().setStatus(FAILED);
            response.put(MESSAGE, "Professional details entry is unavailable for the user");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return null;
        }
        JsonNode designationNode = professionalDetails.get(DESIGNATION);
        if(designationNode == null ||CollectionUtils.isEmpty(Collections.singleton(designationNode))) {
            response.getParams().setStatus(FAILED);
            response.put(MESSAGE, "Designation is unavailable for the user");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            log.warn("Designation is missing in professional details.");
            return null;
        }
        return designationNode.asText();
    }


    private String fetchUserIdFromToken(String authUserToken, SBApiResponse response) {
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(response, HttpStatus.BAD_REQUEST);
        }
        return userId;
    }

    private void updateErrorDetails(SBApiResponse response, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
        response.setResponseCode(responseCode);
    }
}

