package pseudo.gen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.ac.ut.csis.pflow.geom2.DistanceUtils;
import jp.ac.ut.csis.pflow.geom2.ILonLat;
import jp.ac.ut.csis.pflow.geom2.LonLat;
import jp.ac.ut.csis.pflow.geom2.TrajectoryUtils;
import jp.ac.ut.csis.pflow.routing4.logic.Dijkstra;
import jp.ac.ut.csis.pflow.routing4.logic.linkcost.LinkCost;
import jp.ac.ut.csis.pflow.routing4.logic.transport.ITransport;
import jp.ac.ut.csis.pflow.routing4.logic.transport.Transport;
import jp.ac.ut.csis.pflow.routing4.res.Link;
import jp.ac.ut.csis.pflow.routing4.res.Network;
import jp.ac.ut.csis.pflow.routing4.res.Node;
import jp.ac.ut.csis.pflow.routing4.res.Route;
import network.BusStopLoader;
import network.DrmLoader;
import network.RailLoader;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import java.util.concurrent.ThreadLocalRandom;
import pseudo.acs.DataAccessor;
import pseudo.acs.PersonAccessor;
import pseudo.res.*;
import utils.ConfigLoader;
import utils.ParamGroupLoader;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TripGenerator_WebAPI_refactor {

    private final Network drm;
	private final Network railway;

	private final SSLContext sslContext;
	private final PoolingHttpClientConnectionManager connManager;
	private final CloseableHttpClient httpClient;
	private volatile String sessionId;
	private volatile String sessionCookieHeader;
	private volatile long sessionCreatedAt;
	private final long sessionRefreshIntervalMs;

	private final double MIN_TRANSIT_DISTANCE;
	private final double FARE_PER_KILOMETER;
	private final double FARE_PER_HOUR;
	private final double FATIGUE_INDEX_WALK;
	private final double FATIGUE_INDEX_BICYCLE;
	private final double FARE_INIT;
	private final double CAR_AVAILABILITY;
	private final String appDate;
	private final String maxRadius;
	private final String maxRoutes;
	private final String transportCode;
	private final String transitSelection;
	private final double transferPenalty;

	// Tour-based mode choice and extra car cost for target prefectures
	// (docs/TOUR_MODE_CHOICE_DESIGN_ja.md). All default to off = legacy behaviour.
	private final boolean tourEnabled;
	private final boolean carAvailPerPerson;
	private final Set<String> extraCostPrefs;
	private final double extraParking;
	private final double extraDelayPerTrip;
	private final boolean costLogEnabled;
	private final ConcurrentLinkedQueue<String> costLogRows = new ConcurrentLinkedQueue<>();
	private final AtomicInteger tourSimpleCount = new AtomicInteger();
	private final AtomicInteger tourComplexCount = new AtomicInteger();
	private final AtomicInteger modeChangedCount = new AtomicInteger();

	// Mixed-route diagnostic counters (shared across TripTask threads)
	private final AtomicInteger mixedQueryCount = new AtomicInteger();
	private final AtomicInteger mixedNoStationCount = new AtomicInteger();
	private final AtomicInteger mixedNoFareCount = new AtomicInteger();
	private final AtomicInteger mixedTransitAvailable = new AtomicInteger();
	private final AtomicInteger mixedSelectedCount = new AtomicInteger();
	private final AtomicInteger mixedBelowDistThreshold = new AtomicInteger();
	private final AtomicInteger mixedCandidateTotal = new AtomicInteger();
	private final AtomicInteger mixedCandidateBusOnly = new AtomicInteger();
	private final AtomicInteger mixedCandidateTrainOnly = new AtomicInteger();
	private final AtomicInteger mixedCandidateMixed = new AtomicInteger();
	private final AtomicInteger selectedBusOnly = new AtomicInteger();
	private final AtomicInteger selectedTrainOnly = new AtomicInteger();
	private final AtomicInteger selectedMixed = new AtomicInteger();

	// Feature A: Transit stop reachability precheck
	private final Network transitStops;
	private final double precheckRadius;
	private final AtomicInteger precheckSkipCount = new AtomicInteger();
	private final AtomicInteger precheckPassCount = new AtomicInteger();

	// Feature B: GetMixedRoute response cache
	// Configurable via routeCache.maxEntries (default 10000, 0 = disabled)
	private final ConcurrentHashMap<String, List<JsonNode>> routeCache = new ConcurrentHashMap<>();
	private final int routeCacheMaxEntries;
	private final AtomicInteger cacheHitCount = new AtomicInteger();
	private final AtomicInteger cacheMissCount = new AtomicInteger();
	private final AtomicInteger cacheEvictCount = new AtomicInteger();
	private final AtomicLong cacheSavedMs = new AtomicLong();

	// Session lifecycle diagnostics
	private final AtomicInteger sessionRefreshCount = new AtomicInteger();
	private final AtomicInteger sessionRetrySuccessCount = new AtomicInteger();
	private final AtomicInteger sessionRetryFailCount = new AtomicInteger();

	public TripGenerator_WebAPI_refactor(Country japan, Network drm, Network railway) throws Exception {
		this(japan, drm, railway, null);
	}

	public TripGenerator_WebAPI_refactor(Country japan, Network drm, Network railway, Network transitStops) throws Exception {
		super();
        this.drm = drm;
		this.railway = railway;
		this.MIN_TRANSIT_DISTANCE = Double.parseDouble(prop.getProperty("min.transit.distance", "1000"));
		this.FARE_PER_KILOMETER = Double.parseDouble(prop.getProperty("fare.per.kilometer", "10"));
		this.FARE_PER_HOUR = Double.parseDouble(prop.getProperty("fare.per.hour", "1000"));
		this.FATIGUE_INDEX_WALK = Double.parseDouble(prop.getProperty("fatigue.walk", "1.5"));
		this.FATIGUE_INDEX_BICYCLE = Double.parseDouble(prop.getProperty("fatigue.bicycle", "1.2"));
		this.FARE_INIT = Double.parseDouble(prop.getProperty("fare.init", "200"));
		this.CAR_AVAILABILITY = Double.parseDouble(prop.getProperty("car.availability", "0.4"));
		this.appDate = prop.getProperty("api.appDate", "20240401");
		this.maxRadius = prop.getProperty("api.maxRadius", "1000");
		this.maxRoutes = prop.getProperty("api.maxRoutes", "6");
		this.transportCode = prop.getProperty("api.transportCode", "3");
		if (!"1".equals(transportCode) && !"3".equals(transportCode)) {
			throw new IllegalArgumentException("api.transportCode must be 1 (train) or 3 (bus), got: " + transportCode);
		}
		this.transitSelection = prop.getProperty("api.transit.selection", "generalized_cost");
		if (!transitSelection.matches("generalized_cost|min_time|min_fare|min_transfers")) {
			throw new IllegalArgumentException("api.transit.selection must be one of: generalized_cost, min_time, min_fare, min_transfers; got: " + transitSelection);
		}
		this.transferPenalty = Double.parseDouble(prop.getProperty("api.transit.transferPenalty", "0"));
		this.tourEnabled = Boolean.parseBoolean(prop.getProperty("tour.enabled", "false"));
		this.carAvailPerPerson = Boolean.parseBoolean(prop.getProperty("car.availability.perPerson", "false"));
		this.extraCostPrefs = new HashSet<>();
		for (String p : prop.getProperty("car.extraCost.targetPrefs", "13").split(",")) {
			if (!p.trim().isEmpty()) this.extraCostPrefs.add(String.format("%02d", Integer.parseInt(p.trim())));
		}
		this.extraParking = Double.parseDouble(prop.getProperty("car.extraCost.parking", "0"));
		this.extraDelayPerTrip = Double.parseDouble(prop.getProperty("car.extraCost.delayPerTrip", "0"));
		this.costLogEnabled = Boolean.parseBoolean(prop.getProperty("debug.costLog", "false"));
		this.transitStops = transitStops;
		this.precheckRadius = Double.parseDouble(maxRadius);
		this.routeCacheMaxEntries = Integer.parseInt(
			System.getProperty("routeCache.maxEntries",
				prop.getProperty("routeCache.maxEntries", "10000")));
		this.sessionRefreshIntervalMs = (long) (Double.parseDouble(
				prop.getProperty("api.sessionRefreshMinutes", "15")) * 60 * 1000);
		this.sslContext = createSSLContext();
		this.connManager = createConnManager();
		this.httpClient = createHttpClient();
		this.sessionId = createSession();
		this.sessionCreatedAt = System.currentTimeMillis();
	}

	private SSLContext createSSLContext() throws Exception {
		return SSLContextBuilder.create()
				.loadTrustMaterial(new TrustSelfSignedStrategy())
				.build();
	}

	private PoolingHttpClientConnectionManager createConnManager() {
		SSLContext sslContext;
		try {
			sslContext = SSLContextBuilder.create()
					.loadTrustMaterial(new TrustSelfSignedStrategy())
					.build();
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize SSL context", e);
		}

		SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
				sslContext,
				new String[]{"TLSv1.2", "TLSv1.3"},
				null,
				NoopHostnameVerifier.INSTANCE);

		PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(
				RegistryBuilder.<ConnectionSocketFactory>create()
						.register("https", sslSocketFactory)
						.register("http", PlainConnectionSocketFactory.INSTANCE)
						.build());

		connManager.setMaxTotal(32); // Adjust based on your expected total number of concurrent connections
		connManager.setDefaultMaxPerRoute(100); // Adjust per route limits based on your API and use case

		return connManager;
	}
	private CloseableHttpClient createHttpClient() {

		return HttpClients.custom()
				.setSSLContext(this.sslContext)
				.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
				.setConnectionManager(this.connManager)
				.setDefaultRequestConfig(RequestConfig.custom()
						.setCookieSpec(CookieSpecs.IGNORE_COOKIES)
						.build())
				.build();
	}

	private static HttpResponse executePostRequest(CloseableHttpClient httpClient, HttpPost postRequest) throws Exception {
		return httpClient.execute(postRequest);
	}

	public String createSession() throws Exception{

		String createSessionURL = prop.getProperty("api.createSessionURL");
		if (createSessionURL == null) {
			throw new IllegalStateException("Missing config key: api.createSessionURL");
		}
		HttpPost createSessionPost = new HttpPost(createSessionURL);

		String apiUser = System.getenv("PFLOW_API_USER");
		String apiPass = System.getenv("PFLOW_API_PASS");
		if (apiUser == null || apiUser.isBlank()) {
			throw new IllegalStateException("Environment variable PFLOW_API_USER is not set");
		}
		if (apiPass == null || apiPass.isBlank()) {
			throw new IllegalStateException("Environment variable PFLOW_API_PASS is not set");
		}
		List<NameValuePair> sessionParams = new ArrayList<>();
		sessionParams.add(new BasicNameValuePair("UserID", apiUser));
		sessionParams.add(new BasicNameValuePair("Password", apiPass));
		createSessionPost.setEntity(new UrlEncodedFormEntity(sessionParams));

		System.out.println("[session] CreateSession → " + createSessionURL);
		HttpResponse sessionResponse = executePostRequest(this.httpClient, createSessionPost);
		int httpStatus = sessionResponse.getStatusLine().getStatusCode();
		if (httpStatus == 200) {
			String sessionResponseBody = EntityUtils.toString(sessionResponse.getEntity());
			System.out.println("[session] Response body: " + sessionResponseBody.trim());
			// Build explicit Cookie header from all Set-Cookie responses.
			// The cookie jar cannot be relied on when the relay is HTTP-only:
			// if the backend sets Secure on its cookies, a compliant jar will
			// refuse to replay them over http://.
			org.apache.http.Header[] setCookies = sessionResponse.getHeaders("Set-Cookie");
			StringBuilder cookieParts = new StringBuilder();
			for (org.apache.http.Header h : setCookies) {
				String val = h.getValue();
				// Extract "name=value" (everything before the first ";")
				String nameValue = val.contains(";") ? val.substring(0, val.indexOf(';')).trim() : val.trim();
				System.out.println("[session] Server Set-Cookie: " + val);
				if (cookieParts.length() > 0) cookieParts.append("; ");
				cookieParts.append(nameValue);
			}
			this.sessionCookieHeader = cookieParts.toString();
			System.out.println("[session] Cookie header for subsequent calls: " + this.sessionCookieHeader);

			String[] parts = sessionResponseBody.split(",");
			String sessionId = (parts.length > 1 ? parts[1] : parts[0]).trim().replace("\r", "").replace("\n", "");
			if (sessionId.isEmpty()) {
				throw new RuntimeException("[session] CreateSession returned HTTP 200 but session ID is empty. "
					+ "Raw response: " + sessionResponseBody);
			}
			System.out.println("[session] Active session ID: " + sessionId);
			return sessionId;
		} else {
			String body = "";
			try { body = EntityUtils.toString(sessionResponse.getEntity()); } catch (Exception ignored) {}
			throw new RuntimeException("[session] CreateSession failed: HTTP " + httpStatus
				+ " from " + createSessionURL + " — body: " + body.trim());
		}
	}

	/**
	 * Proactive session refresh: called before an API request when the session
	 * age exceeds sessionRefreshIntervalMs. Only one thread performs the refresh;
	 * others see the updated volatile sessionId.
	 *
	 * @return the current (possibly refreshed) session ID
	 */
	private String getValidSession() {
		if (System.currentTimeMillis() - sessionCreatedAt > sessionRefreshIntervalMs) {
			return refreshSession("proactive (age > " + sessionRefreshIntervalMs / 60000 + " min)");
		}
		return sessionId;
	}

	/**
	 * Force a session refresh. Synchronized so only one thread creates a new
	 * session; others wait and return the fresh ID.
	 *
	 * @param reason human-readable reason for the refresh (logged)
	 * @return new session ID
	 * @throws RuntimeException if the refresh fails (fail-fast, no silent fallback)
	 */
	private synchronized String refreshSession(String reason) {
		// Double-check: another thread may have already refreshed while we waited
		long age = System.currentTimeMillis() - sessionCreatedAt;
		if (age < sessionRefreshIntervalMs / 2) {
			return sessionId; // recently refreshed by another thread
		}
		System.out.println("[session] Refreshing session (" + reason + "), age=" + age / 1000 + "s");
		try {
			String newId = createSession();
			this.sessionId = newId;
			this.sessionCreatedAt = System.currentTimeMillis();
			sessionRefreshCount.incrementAndGet();
			System.out.println("[session] Refresh successful (total refreshes: " + sessionRefreshCount.get() + ")");
			return newId;
		} catch (Exception e) {
			throw new RuntimeException("[session] Refresh failed (" + reason + "): " + e.getMessage(), e);
		}
	}

	protected double getRandom() {
		return ThreadLocalRandom.current().nextDouble();
	}
	
	private class TripTask implements Callable<Integer> {
		private int id;
		private final List<Person> listAgents;
		private int error;
		private int total;
		private int tripCounter;
		LinkCost linkCost = new LinkCost();
		Dijkstra routing = new Dijkstra(linkCost);

		public TripTask(int id, List<Person> listAgents){
			this.id = id;
			this.listAgents = listAgents;
			this.total = error = 0;
			this.tripCounter = 0;
		}	
		
		private EPurpose convertHomeMode(ELabor labor) {
			switch(labor) {
			case WORKER:
				return EPurpose.OFFICE;
			case JOBLESS:
			case NO_LABOR:
			case UNDEFINED:
			case INFANT:
				return EPurpose.FREE;
			case PRE_SCHOOL:
			case PRIMARY_SCHOOL:
			case SECONDARY_SCHOOL:
			case HIGH_SCHOOL:
			case COLLEGE:
			case JUNIOR_COLLEGE:
			default:
				return EPurpose.SCHOOL;
			}
		}

		private ETransport getTransport(int mode) { // mode defined by WebAPI
			switch (mode) {
				case 4:		return ETransport.WALK;
				case 3:	return ETransport.BUS;
                case 2:		return ETransport.TRAIN;
				case 0: return ETransport.NOT_DEFINED;
				default:		return ETransport.CAR;
			}
		}

		private int calculateMultiplier(ETransport mode) {
			switch (mode) {
				case WALK: return 6;
				case BICYCLE: return 3;
				case CAR: return 1;
				default: return 1;
			}
		}

		private void configureCalendar(Calendar calendar, Date date) {
			TimeZone timeZone = TimeZone.getTimeZone("Asia/Tokyo");
			calendar.setTime(date);
			calendar.setTimeZone(timeZone);
			calendar.add(Calendar.MILLISECOND, -timeZone.getOffset(calendar.getTimeInMillis()));
			// Set trajectory output to target calendar year and month directly
			int baseYear = Integer.parseInt(prop.getProperty("trajectory.baseYear", "2020"));
			calendar.set(Calendar.YEAR, baseYear);
			calendar.set(Calendar.MONTH, Calendar.OCTOBER);
		}

		/** Legacy entry point: evaluate all modes and pick the cheapest for a single trip. */
		private ETransport determineTransportMode(Person person, double distance, Route route, Map<String, String> mixedparams, JsonNode[] mixedResultsHolder){
			return chooseMin(evaluateOptions(person, null, distance, route, mixedparams, mixedResultsHolder));
		}

		/** Mode with the lowest generalized cost (ties: first inserted, as before). */
		private ETransport chooseMin(Map<ETransport, Double> choices) {
			return choices.entrySet()
					.stream()
					.min(Comparator.comparing(Map.Entry::getValue))
					.map(Map.Entry::getKey)
					.orElse(ETransport.NOT_DEFINED);
		}

		/**
		 * Generalized cost (yen) of every available mode for one trip, without choosing.
		 * The body is the former determineTransportMode() minus the final min selection.
		 *
		 * @param carAvailable null = draw car availability per trip (legacy behaviour);
		 *                     otherwise the availability already drawn once for this person.
		 */
		private Map<ETransport, Double> evaluateOptions(Person person, Boolean carAvailable, double distance, Route route, Map<String, String> mixedparams, JsonNode[] mixedResultsHolder){
			Map<ETransport, Double> choices = new LinkedHashMap<>();

			if(route!=null){
				double roadtime = route.getCost(); // seconds
				double roadfare = FARE_INIT + route.getLength() / 1000 * FARE_PER_KILOMETER; // length in meters, 150 as initial cost to avoid short distance car travel
				double roadcost = roadfare + roadtime / 3600 * FARE_PER_HOUR;
				boolean car = (carAvailable != null) ? carAvailable : (person.hasCar() || getRandom() < CAR_AVAILABILITY);
				if(car){
					choices.put(ETransport.CAR, roadcost);
				}

				double walktime = route.getLength() / 1.38;
				double walkcost = walktime / 3600 * FARE_PER_HOUR * FATIGUE_INDEX_WALK;
				choices.put(ETransport.WALK, walkcost);

				if(person.hasBike()){
					double biketime = walktime / 2;
					double bikecost = biketime / 3600 * FARE_PER_HOUR * FATIGUE_INDEX_BICYCLE;
					choices.put(ETransport.BICYCLE, bikecost);
				}
			}

			if(distance>MIN_TRANSIT_DISTANCE){
				// Feature A: Transit stop reachability precheck
				boolean transitReachable = true;
				if (transitStops != null) {
					double oLon = Double.parseDouble(mixedparams.get("StartLongitude"));
					double oLat = Double.parseDouble(mixedparams.get("StartLatitude"));
					double dLon = Double.parseDouble(mixedparams.get("GoalLongitude"));
					double dLat = Double.parseDouble(mixedparams.get("GoalLatitude"));
					boolean originHasStop = !transitStops.queryNode(oLon, oLat, precheckRadius).isEmpty();
					boolean destHasStop = !transitStops.queryNode(dLon, dLat, precheckRadius).isEmpty();
					if (!originHasStop || !destHasStop) {
						transitReachable = false;
						precheckSkipCount.incrementAndGet();
					} else {
						precheckPassCount.incrementAndGet();
					}
				}

				if (transitReachable) {
					mixedQueryCount.incrementAndGet();

					// Feature B: Response cache (disabled when routeCache.maxEntries=0)
					String cacheKey = (routeCacheMaxEntries > 0) ? buildCacheKey(mixedparams) : null;
					List<JsonNode> candidates = (cacheKey != null) ? routeCache.get(cacheKey) : null;
					if (candidates != null) {
						cacheHitCount.incrementAndGet();
					} else {
						cacheMissCount.incrementAndGet();
						long t0 = System.currentTimeMillis();
						// Proactive refresh: get a session that hasn't expired yet
						String sid = getValidSession();
						candidates = getMixedRoutes(httpClient, sid, sessionCookieHeader, mixedparams);

						// null = non-200 HTTP → session likely expired → refresh + retry once
						if (candidates == null) {
							String freshSid = refreshSession("HTTP non-200 on getMixedRoutes");
							candidates = getMixedRoutes(httpClient, freshSid, sessionCookieHeader, mixedparams);
							if (candidates != null) {
								sessionRetrySuccessCount.incrementAndGet();
							} else {
								sessionRetryFailCount.incrementAndGet();
								throw new RuntimeException(
									"[API FAILURE] GetMixedRoute returned non-200 even after session refresh. "
									+ "API is unavailable or session is permanently invalid. Stopping run.");
							}
						}
						cacheSavedMs.addAndGet(System.currentTimeMillis() - t0);
						if (routeCacheMaxEntries > 0) {
							if (routeCache.size() >= routeCacheMaxEntries) {
								// Simple eviction: clear the entire cache when full.
								// LRU would be more efficient but adds complexity;
								// a full clear is safe and simple.
								routeCache.clear();
								cacheEvictCount.incrementAndGet();
							}
							routeCache.put(cacheKey, candidates);
						}
					}

					mixedResultsHolder[0] = selectBestTransitCandidate(candidates);
					if (mixedResultsHolder[0] == null) {
						mixedNoStationCount.incrementAndGet();
					} else {
						mixedTransitAvailable.incrementAndGet();
						double mixedfare = mixedResultsHolder[0].get("fare").asDouble();
						double mixedtime = mixedResultsHolder[0].get("total_time").asDouble();
						double mixedcost = mixedfare + mixedtime / 60 * FARE_PER_HOUR;
						choices.put(ETransport.MIX, mixedcost);
					}
				}
			} else {
				mixedBelowDistThreshold.incrementAndGet();
			}

			return choices;
		}

		private JsonNode selectBestTransitCandidate(List<JsonNode> candidates) {
			JsonNode best = null;
			double bestScore = Double.MAX_VALUE;

			for (JsonNode candidate : candidates) {
				int numStation = candidate.path("num_station").asInt();
				int fare = candidate.path("fare").asInt();
				if (numStation <= 0 || fare <= 0) continue;

				// Classify candidate
				mixedCandidateTotal.incrementAndGet();
				boolean hasBus = false, hasTrain = false;
				for (JsonNode feature : candidate.path("features")) {
					int t = feature.path("properties").path("transportation").asInt();
					if (t == 3) hasBus = true;
					if (t == 2) hasTrain = true;
				}
				if (hasBus && hasTrain) mixedCandidateMixed.incrementAndGet();
				else if (hasBus) mixedCandidateBusOnly.incrementAndGet();
				else if (hasTrain) mixedCandidateTrainOnly.incrementAndGet();

				double score;
				double time = candidate.path("total_time").asDouble();
				int transfers = Math.max(0, numStation - 2); // stations minus origin+destination
				switch (transitSelection) {
					case "min_time":
						score = time;
						break;
					case "min_fare":
						score = fare;
						break;
					case "min_transfers":
						score = transfers + time * 0.001; // tiebreak by time
						break;
					default: // generalized_cost
						score = fare + time / 60.0 * FARE_PER_HOUR + transfers * transferPenalty;
						break;
				}

				if (score < bestScore) {
					bestScore = score;
					best = candidate;
				}
			}

			// Classify selected candidate
			if (best != null) {
				boolean hasBus = false, hasTrain = false;
				for (JsonNode feature : best.path("features")) {
					int t = feature.path("properties").path("transportation").asInt();
					if (t == 3) hasBus = true;
					if (t == 2) hasTrain = true;
				}
				if (hasBus && hasTrain) selectedMixed.incrementAndGet();
				else if (hasBus) selectedBusOnly.incrementAndGet();
				else if (hasTrain) selectedTrainOnly.incrementAndGet();
			}

			return best;
		}

		// Methods to refactor and modularize the code
		private long calculateTravelTime(Route route, int multiplier) {
			if (route != null) {
				return (long) route.getCost() * multiplier;
			} else {
				return 3600L;
			}
		}

		private void addSubpoints(List<Node> nodes, Map<Node, Date> timeMap, ETransport nextMode, EPurpose purpose, List<SPoint> subpoints) {
			for (ILonLat node : nodes) {
				Date date = timeMap.get(node);
				Calendar cl = Calendar.getInstance();
				configureCalendar(cl, date);
				date = cl.getTime();
				SPoint point = new SPoint(node.getLon(), node.getLat(), date, nextMode, purpose);
				subpoints.add(point);
			}
		}

		private void assignLinksToSubpoints(List<SPoint> subpoints, List<Link> links) {
			for (int k = 1; k <= links.size(); k++) {
				subpoints.get(k).setLink(links.get(k - 1).getLinkID());
			}
		}

		private List<Trip> handleMixedTransport(ETransport nextMode, EPurpose purpose, JsonNode[] mixedResultsHolder, List<SPoint> subpoints, List<SPoint> points, Activity next, Route route, long startTime, long endTime, LonLat oll, LonLat dll) {
			long mixedTime = mixedResultsHolder[0].path("total_time").asLong() * 60;
			endTime += mixedTime;
			long travelTime = mixedTime;
			long depTime = next.getStartTime() - travelTime;

			List<ETransport> nodeModes = new ArrayList<>();
			List<Node> nodes = extractNodesFromMixedResults(mixedResultsHolder, nodeModes);
			boolean publicTransit = mixedResultsHolder[0].path("num_station").asInt() > 0;
			List<JsonNode> currentSubtrip = new ArrayList<>();
			int lastMode = determineInitialTransportMode(mixedResultsHolder);

			List<Trip> mixedTrips = processMixedTransportFeatures(mixedResultsHolder, currentSubtrip, lastMode, publicTransit, depTime, purpose);
			if (!nodes.isEmpty()) {
				addTimeStampedSubpoints(nodes, nodeModes, startTime, endTime, purpose, subpoints);
			}
			points.addAll(subpoints);
			return mixedTrips;
		}

//		private List<Node> extractNodesFromMixedResults(JsonNode[] mixedResultsHolder) {
//			List<Node> nodes = new ArrayList<>();
//			JsonNode routeData = mixedResultsHolder[0].path("features");
//			for (JsonNode feature : routeData) {
//				JsonNode coordinates = feature.path("geometry").path("coordinates");
//				String stationName =   feature.path("properties").path("station").toString();
//				if (coordinates.isArray()) {
//					double lon = coordinates.get(0).asDouble();
//					double lat = coordinates.get(1).asDouble();
//					nodes.add(new Node(feature.path("properties").path("id").toString(), lon, lat));
//				} else {
//					System.out.println("Coordinate from WebAPI is not an array!!");
//				}
//			}
//			return nodes;
//		}

		private List<Node> extractNodesFromMixedResults(JsonNode[] mixedResultsHolder, List<ETransport> nodeModes) {
			List<Node> nodes = new ArrayList<>();
			Node firstStationNode = null;
			Node lastStationNode = null;

			JsonNode routeData = mixedResultsHolder[0].path("features");
			boolean previousHasStation = false;
			int previousTransportation = -1;

			for (JsonNode feature : routeData) {
				JsonNode coordinates = feature.path("geometry").path("coordinates");
				String stationName = feature.path("properties").path("station").asText();
				int currentTransportation = feature.path("properties").path("transportation").asInt();
				boolean currentHasStation = !"null".equals(stationName);

				if ((previousHasStation && !currentHasStation) ||
					(previousTransportation != -1 && previousTransportation != currentTransportation)) {

					if (firstStationNode != null && lastStationNode != null && !firstStationNode.equals(lastStationNode)){
						LinkCost linkCost = new LinkCost(Transport.RAILWAY);
						Dijkstra routing = new Dijkstra(linkCost);
						Route route = routing.getRoute(railway,	firstStationNode.getLon(), firstStationNode.getLat(), lastStationNode.getLon(), lastStationNode.getLat());

						if (route != null && route.numNodes() > 0){
							List<Node> rail_nodes = route.listNodes();
							nodes.addAll(rail_nodes);
							for (int j = 0; j < rail_nodes.size(); j++) nodeModes.add(ETransport.TRAIN);
						}
					}
					firstStationNode = null;
					lastStationNode = null;
				}

				if (!currentHasStation && coordinates.isArray()) {
					double lon = coordinates.get(0).asDouble();
					double lat = coordinates.get(1).asDouble();
					Node node = new Node(feature.path("properties").path("id").asText(), lon, lat);
					nodes.add(node);
					nodeModes.add(getTransport(currentTransportation));
				}

				if (currentHasStation && coordinates.isArray()) {
					double lon = coordinates.get(0).asDouble();
					double lat = coordinates.get(1).asDouble();
					Node node = new Node(feature.path("properties").path("id").asText(), lon, lat);

					if (firstStationNode == null || previousTransportation != currentTransportation) {
						firstStationNode = node;
					}
					lastStationNode = node;
				}

				previousHasStation = currentHasStation;
				previousTransportation = currentTransportation;
			}

			// process final station segment
			if (firstStationNode != null && lastStationNode != null && !firstStationNode.equals(lastStationNode)) {
				LinkCost linkCost = new LinkCost(Transport.RAILWAY);
				Dijkstra routing = new Dijkstra(linkCost);
				Route route = routing.getRoute(railway, firstStationNode.getLon(), firstStationNode.getLat(), lastStationNode.getLon(), lastStationNode.getLat());
				if (route != null && route.numNodes() > 0) {
					List<Node> rail_nodes = route.listNodes();
					nodes.addAll(rail_nodes);
					for (int j = 0; j < rail_nodes.size(); j++) nodeModes.add(ETransport.TRAIN);
				}
			}
			// nodes may be empty for bus-only routes without railway network coverage
			return nodes;
		}

		private int determineInitialTransportMode(JsonNode[] mixedResultsHolder) {
			return mixedResultsHolder[0].path("features").get(0).path("properties").path("transportation").asInt();
		}

		private List<Trip> processMixedTransportFeatures(JsonNode[] mixedResultsHolder, List<JsonNode> currentSubtrip, int lastMode, boolean publicTransit, long depTime, EPurpose purpose) {
			List<Trip> trips = new ArrayList<>();
			JsonNode routeData = mixedResultsHolder[0].path("features");
			JsonNode prevNode = null;
			long currentTime = depTime;
			for (JsonNode feature : routeData) {
				int currentMode = feature.path("properties").path("transportation").asInt();

				if (currentMode != lastMode) {
					if (currentSubtrip.size() > 1) {
						currentTime += estimateSegmentTime(currentSubtrip, lastMode);
						Trip t = buildTripForSubtrip(currentSubtrip, lastMode, publicTransit, currentTime, purpose);
						if (t != null) trips.add(t);
					}
					currentSubtrip = new ArrayList<>();
					currentSubtrip.add(prevNode);
					lastMode = currentMode;
				}
				currentSubtrip.add(feature);
				prevNode = feature;
			}
			// emit final segment
			if (currentSubtrip.size() > 1) {
				currentTime += estimateSegmentTime(currentSubtrip, lastMode);
				Trip t = buildTripForSubtrip(currentSubtrip, lastMode, publicTransit, currentTime, purpose);
				if (t != null) trips.add(t);
			}
			return trips;
		}

		private long estimateSegmentTime(List<JsonNode> subtrip, int mode) {
			JsonNode first = subtrip.get(0).path("geometry").path("coordinates");
			JsonNode last = subtrip.get(subtrip.size() - 1).path("geometry").path("coordinates");
			if (!first.isArray() || !last.isArray()) return 300L;
			LonLat a = new LonLat(first.get(0).asDouble(), first.get(1).asDouble());
			LonLat b = new LonLat(last.get(0).asDouble(), last.get(1).asDouble());
			double distance = DistanceUtils.distance(a, b);
			double speed = Speed.get(getTransport(mode));
			if (speed <= 0) speed = Speed.WALK;
			return Math.max(1L, (long)(distance / speed));
		}

		private Trip buildTripForSubtrip(List<JsonNode> currentSubtrip, int lastMode, boolean publicTransit, long depTime, EPurpose purpose) {
			JsonNode ollCoords = currentSubtrip.get(0).path("geometry").path("coordinates");
			JsonNode dllCoords = currentSubtrip.get(currentSubtrip.size() - 1).path("geometry").path("coordinates");

			// 2025-02-20 feature railway interpolation with 1.2 algorithm
			int firstMode = currentSubtrip.get(0).path("properties").path("transportation").asInt();
			boolean allSame = true, hasMode2 = false, hasMode3 = false;

			for (JsonNode node : currentSubtrip) {
				int transportation = node.path("properties").path("transportation").asInt();
				if (transportation != firstMode) allSame = false;
				if (transportation == 2) hasMode2 = true;
				if (transportation == 3) hasMode3 = true;
			}

			int finalMode = allSame ? firstMode : (hasMode2 ? 2 : (hasMode3 ? 3 : lastMode));
			ETransport mode = getTransport(finalMode);

			if (mode == ETransport.CAR && publicTransit) {
				mode = ETransport.WALK;
			}
			if (ollCoords.isArray() && dllCoords.isArray()) {
				LonLat moll = new LonLat(ollCoords.get(0).asDouble(), ollCoords.get(1).asDouble());
				LonLat mdll = new LonLat(dllCoords.get(0).asDouble(), dllCoords.get(1).asDouble());
				return new Trip(mode, purpose, depTime, moll, mdll);
			} else {
				System.out.println("No coordinate from API!");
				return null;
			}
		}

		private void addTimeStampedSubpoints(List<Node> nodes, long startTime, long endTime, ETransport nextMode, EPurpose purpose, List<SPoint> subpoints) {
			Map<Node, Date> timeMap = TrajectoryUtils.putTimeStamp(nodes, new Date(startTime * 1000), new Date(endTime * 1000));
			addSubpoints(nodes, timeMap, nextMode, purpose, subpoints);
		}

		private void addTimeStampedSubpoints(List<Node> nodes, List<ETransport> nodeModes, long startTime, long endTime, EPurpose purpose, List<SPoint> subpoints) {
			Map<Node, Date> timeMap = TrajectoryUtils.putTimeStamp(nodes, new Date(startTime * 1000), new Date(endTime * 1000));
			addSubpoints(nodes, timeMap, nodeModes, purpose, subpoints);
		}

		private void addSubpoints(List<Node> nodes, Map<Node, Date> timeMap, List<ETransport> nodeModes, EPurpose purpose, List<SPoint> subpoints) {
			for (int idx = 0; idx < nodes.size(); idx++) {
				Node node = nodes.get(idx);
				Date date = timeMap.get(node);
				Calendar cl = Calendar.getInstance();
				configureCalendar(cl, date);
				date = cl.getTime();
				ETransport mode = idx < nodeModes.size() ? nodeModes.get(idx) : ETransport.MIX;
				SPoint point = new SPoint(node.getLon(), node.getLat(), date, mode, purpose);
				subpoints.add(point);
			}
		}

		public String convertSecondsToHHMM(double totalSeconds) {
			// Calculate hours and minutes
			int hours = (int) (totalSeconds / 3600);
			int minutes = (int) ((totalSeconds % 3600) / 60);

			// Format hours and minutes to HHMM as WebAPI requests
			return String.format("%02d%02d", hours, minutes);
		}

		private int process(Person person) {
			List<SPoint> points = new ArrayList<>();

			List<Activity> activities = person.getActivities();
			Activity pre = activities.get(0);

			try {
				if (activities.size() == 1) {
					Trip trip = new Trip(ETransport.NOT_DEFINED, EPurpose.HOME, 0, pre.getLocation(), pre.getLocation());
					trip.setTripId(++tripCounter);
					trip.setSubtripId(0);
					person.addTrip(trip);

					Calendar cl = Calendar.getInstance();
					Date startDate = new Date(0);
					configureCalendar(cl, startDate);
					startDate = cl.getTime();
					points.add(new SPoint(pre.getLocation().getLon(), pre.getLocation().getLat(), startDate, ETransport.NOT_DEFINED, EPurpose.HOME));
					Date endDate = new Date(86399000);
					configureCalendar(cl, endDate);
					endDate = cl.getTime();
					points.add(new SPoint(pre.getLocation().getLon(), pre.getLocation().getLat(), endDate, ETransport.NOT_DEFINED, EPurpose.HOME));
					person.addTrajectory(points);
				} else {
					// Car availability: legacy = drawn per trip inside evaluateOptions (null);
					// tour mode or car.availability.perPerson = drawn once per person for the day.
					Boolean carToday = (tourEnabled || carAvailPerPerson)
							? Boolean.valueOf(person.hasCar() || getRandom() < CAR_AVAILABILITY) : null;

					// (1) generalized cost of every mode for every trip of the day
					List<Leg> legs = new ArrayList<>();
					for (int i = 1; i < activities.size(); i++) {
						legs.add(evaluateLeg(person, activities.get(i - 1), activities.get(i), carToday));
					}
					// (2) mode decision: trip by trip (legacy) or tour by tour
					if (tourEnabled) {
						decideByTour(legs);
					} else {
						decideByTrip(legs);
					}
					// (3) trips and trajectory with the decided modes
					for (Leg leg : legs) {
						buildLeg(person, leg, points);
					}
					if (costLogEnabled) {
						logLegs(person, legs);
					}
				}
				person.addTrajectory(points);
			} catch (Exception e){
				throw new RuntimeException("Exception processing person: " + person, e);
			}

			return 0;
		}

		/** Step (1): route, costs of every mode and target-prefecture flags for one trip. */
		private Leg evaluateLeg(Person person, Activity pre, Activity next, Boolean carToday) {
			Leg leg = new Leg(pre, next, ++tripCounter);
			leg.distance = DistanceUtils.distance(pre.getLocation(), next.getLocation());
			if (leg.distance > 0) {
				GLonLat oll = pre.getLocation();
				GLonLat dll = next.getLocation();
				Map<String, String> mixedparams = getStringStringMap(oll, dll, next.getStartTime());
				leg.route = routing.getRoute(drm, oll.getLon(), oll.getLat(), dll.getLon(), dll.getLat());
				leg.costs = evaluateOptions(person, carToday, leg.distance, leg.route, mixedparams, leg.transit);
				leg.baseline = chooseMin(leg.costs);
				leg.origTarget = inTargetPref(pre.getGcode());
				leg.destTarget = inTargetPref(next.getGcode());
			}
			return leg;
		}

		private boolean inTargetPref(String gcode) {
			if (gcode == null || extraCostPrefs.isEmpty()) return false;
			String g = gcode.trim();
			if (g.length() == 4) g = "0" + g;  // prefectures 1-9 without a leading zero
			return g.length() >= 2 && extraCostPrefs.contains(g.substring(0, 2));
		}

		/** Extra cost for the car option of one trip: parking when arriving in a target prefecture, delay when entering or leaving it. */
		private double tripExtraCost(Leg leg, boolean chargeParking) {
			double extra = 0;
			if (chargeParking && leg.destTarget) extra += extraParking;
			if (leg.origTarget || leg.destTarget) extra += extraDelayPerTrip;
			return extra;
		}

		private Map<ETransport, Double> withCarExtra(Map<ETransport, Double> costs, double extra) {
			Map<ETransport, Double> c = new LinkedHashMap<>(costs);
			if (extra > 0 && c.containsKey(ETransport.CAR)) {
				c.put(ETransport.CAR, c.get(ETransport.CAR) + extra);
			}
			return c;
		}

		/** Step (2), legacy: the cheapest mode trip by trip (identical to before when no extra cost is set). */
		private void decideByTrip(List<Leg> legs) {
			for (Leg leg : legs) {
				if (leg.distance <= 0) continue;
				leg.mode = chooseMin(withCarExtra(leg.costs, tripExtraCost(leg, true)));
				leg.tourType = "trip";
			}
		}

		/** Step (2), tour mode: split the day into tours (a tour ends on returning home) and decide each. */
		private void decideByTour(List<Leg> legs) {
			List<Leg> tour = new ArrayList<>();
			for (Leg leg : legs) {
				tour.add(leg);
				if (leg.next.getPurpose() == EPurpose.HOME) {
					decideTour(tour);
					tour = new ArrayList<>();
				}
			}
			if (!tour.isEmpty()) decideTour(tour);
		}

		/**
		 * Simple round trip (home -> A -> home, two moving trips): one mode for both ways, chosen by the
		 * round-trip cost. A car tour touching a target prefecture pays parking once and the delay cost
		 * for every trip that enters or leaves the target.
		 * Other tours: trip by trip, but the car is only usable if the person left home by car, and a
		 * person who left by car goes back home by car. (Forcing one mode on every trip of a longer tour
		 * would make e.g. a 300 m lunch trip decide a 30 km commute.)
		 */
		private void decideTour(List<Leg> tour) {
			List<Leg> moving = new ArrayList<>();
			for (Leg l : tour) {
				if (l.distance > 0) moving.add(l);
			}
			if (moving.isEmpty()) return;

			int nTarget = 0;
			for (Leg l : moving) {
				if (l.origTarget || l.destTarget) nTarget++;
			}
			boolean returnsHome = tour.get(tour.size() - 1).next.getPurpose() == EPurpose.HOME;

			if (moving.size() == 2 && returnsHome) {
				Leg out = moving.get(0);
				Leg back = moving.get(1);
				ETransport best = ETransport.NOT_DEFINED;
				double bestCost = Double.MAX_VALUE;
				for (ETransport m : TOUR_MODES) {
					if (!out.costs.containsKey(m) || !back.costs.containsKey(m)) continue;
					double c = out.costs.get(m) + back.costs.get(m);
					if (m == ETransport.CAR && nTarget > 0) c += extraParking + extraDelayPerTrip * nTarget;
					if (c < bestCost) {
						bestCost = c;
						best = m;
					}
				}
				if (best != ETransport.NOT_DEFINED) {
					out.mode = best;
					back.mode = best;
					out.tourType = "simple";
					back.tourType = "simple";
					tourSimpleCount.incrementAndGet();
					return;
				}
			}

			boolean carOut = false;
			boolean parkingCharged = false;
			for (int k = 0; k < moving.size(); k++) {
				Leg l = moving.get(k);
				Map<ETransport, Double> c = withCarExtra(l.costs, tripExtraCost(l, !parkingCharged));
				if (k > 0 && !carOut) c.remove(ETransport.CAR);
				boolean backHome = (l.next.getPurpose() == EPurpose.HOME);
				l.mode = (carOut && backHome && c.containsKey(ETransport.CAR)) ? ETransport.CAR : chooseMin(c);
				if (k == 0) carOut = (l.mode == ETransport.CAR);
				if (l.mode == ETransport.CAR && l.destTarget) parkingCharged = true;
				l.tourType = "complex";
			}
			tourComplexCount.incrementAndGet();
		}

		/** Step (3): trips and trajectory for one trip with its decided mode (the former loop body of process()). */
		private void buildLeg(Person person, Leg leg, List<SPoint> points) {
			Activity pre = leg.pre;
			Activity next = leg.next;
			GLonLat oll = pre.getLocation();
			GLonLat dll = next.getLocation();
			long startTime = next.getStartTime();
			long endTime = startTime;
			EPurpose purpose = next.getPurpose();
			int currentTripId = leg.tripId;
			List<SPoint> subpoints = new ArrayList<>();

			if (leg.distance > 0) {
				ETransport nextMode = leg.mode;
				if (nextMode != leg.baseline) {
					modeChangedCount.incrementAndGet();
				}
				Route route = leg.route;
				JsonNode[] mixedResultsHolder = leg.transit;

				int multiplier = calculateMultiplier(nextMode);
				long travelTime = 0;

				if (nextMode == ETransport.WALK || nextMode == ETransport.BICYCLE || nextMode == ETransport.CAR) {
					travelTime = calculateTravelTime(route, multiplier);
					endTime += travelTime;

					List<Node> nodes = route.listNodes();
					Map<Node, Date> timeMap = TrajectoryUtils.putTimeStamp(nodes, new Date(startTime * 1000), new Date(endTime * 1000));
					addSubpoints(nodes, timeMap, nextMode, purpose, subpoints);

					List<Link> links = route.listLinks();
					assignLinksToSubpoints(subpoints, links);
					points.addAll(subpoints);

					long depTime = next.getStartTime() - travelTime;
					Trip trip = new Trip(nextMode, purpose, depTime, oll, dll);
					trip.setTripId(currentTripId);
					trip.setSubtripId(0);
					person.addTrip(trip);
				} else if (nextMode == ETransport.MIX) {
					mixedSelectedCount.incrementAndGet();
					if (mixedResultsHolder[0].path("features").get(0) == null) {
						System.out.println("empty mixed results!");
					}
					List<Trip> mixedTrips = handleMixedTransport(nextMode, purpose, mixedResultsHolder, subpoints, points, next, route, startTime, endTime, oll, dll);

					// Assign tripId, subtripId, and compute repMode for mixed subtrips
					ETransport repMode = ETransport.NOT_DEFINED;
					for (int s = 0; s < mixedTrips.size(); s++) {
						Trip t = mixedTrips.get(s);
						t.setTripId(currentTripId);
						t.setSubtripId(s);
						repMode = Trip.computeRepMode(repMode, t.getTransport());
					}
					for (Trip t : mixedTrips) {
						t.setRepMode(repMode);
						person.addTrip(t);
					}
				} else {
					Trip trip = new Trip(ETransport.NOT_DEFINED, next.getPurpose(), next.getStartTime(), pre.getLocation(), pre.getLocation());
					trip.setTripId(currentTripId);
					trip.setSubtripId(0);
					person.addTrip(trip);
					Calendar cl = Calendar.getInstance();
					Date startDate = new Date(next.getStartTime());
					configureCalendar(cl, startDate);
					startDate = cl.getTime();
					points.add(new SPoint(pre.getLocation().getLon(), pre.getLocation().getLat(), startDate, ETransport.NOT_DEFINED, EPurpose.HOME));
					Date endDate = new Date(next.getStartTime() + 300);
					configureCalendar(cl, endDate);
					endDate = cl.getTime();
					points.add(new SPoint(pre.getLocation().getLon(), pre.getLocation().getLat(), endDate, ETransport.NOT_DEFINED, EPurpose.HOME));
				}
			} else {
				Trip trip = new Trip(ETransport.NOT_DEFINED, next.getPurpose(), next.getStartTime(), pre.getLocation(), pre.getLocation());
				trip.setTripId(currentTripId);
				trip.setSubtripId(0);
				person.addTrip(trip);
				Calendar cl = Calendar.getInstance();
				Date startDate = new Date(next.getStartTime());
				configureCalendar(cl, startDate);
				startDate = cl.getTime();
				points.add(new SPoint(pre.getLocation().getLon(), pre.getLocation().getLat(), startDate, ETransport.NOT_DEFINED, next.getPurpose()));
			}
		}

		/** debug.costLog: one row per moving trip with the cost of every mode and the decision. */
		private void logLegs(Person person, List<Leg> legs) {
			for (Leg l : legs) {
				if (l.distance <= 0) continue;
				Map<ETransport, Double> c = l.costs;
				EPurpose p = l.next.getPurpose();
				costLogRows.add(String.join(",",
						String.valueOf(person.getId()),
						String.valueOf(l.tripId),
						String.valueOf(l.pre.getGcode()),
						String.valueOf(l.next.getGcode()),
						p == null ? "" : String.valueOf(p.getId()),
						String.format(Locale.ROOT, "%.0f", l.distance),
						c.containsKey(ETransport.CAR) ? "1" : "0",
						fmtCost(c.get(ETransport.CAR)),
						fmtCost(c.get(ETransport.WALK)),
						fmtCost(c.get(ETransport.BICYCLE)),
						fmtCost(c.get(ETransport.MIX)),
						l.baseline.name(),
						l.mode.name(),
						l.tourType));
			}
		}

		private Map<String, String> getStringStringMap(GLonLat oll, GLonLat dll, long startTime) {
			Map<String, String> params = new HashMap<>();
			params.put("UnitTypeCode", "2");
			params.put("StartLongitude", String.valueOf(oll.getLon()));
			params.put("StartLatitude", String.valueOf(oll.getLat()));
			params.put("GoalLongitude", String.valueOf(dll.getLon()));
			params.put("GoalLatitude", String.valueOf(dll.getLat()));

			Map<String, String> mixedparams = new HashMap<>(params);
			mixedparams.put("TransportCode", transportCode);

			mixedparams.put("AppDate", appDate);
			mixedparams.put("AppTime", convertSecondsToHHMM(startTime));
			mixedparams.put("MaxRadius", maxRadius);
			mixedparams.put("MaxRoutes", maxRoutes);
			return mixedparams;
		}

		/**
		 * Build a cache key from API parameters. Coordinates are bucketed to ~500m
		 * (0.005 degree grid), time to 30-minute intervals. The 500m bucket is within
		 * MaxRadius (1000m), so the API finds largely the same set of nearby stops.
		 */
		private String buildCacheKey(Map<String, String> params) {
			double oLon = Double.parseDouble(params.get("StartLongitude"));
			double oLat = Double.parseDouble(params.get("StartLatitude"));
			double dLon = Double.parseDouble(params.get("GoalLongitude"));
			double dLat = Double.parseDouble(params.get("GoalLatitude"));
			// Round to nearest 0.005 degree ≈ 500m bucket
			String oKey = bucketCoord(oLon) + "," + bucketCoord(oLat);
			String dKey = bucketCoord(dLon) + "," + bucketCoord(dLat);
			// Bucket time to 2-hour intervals: "0730" → "06", "0945" → "08"
			// Transit fares and approximate travel times are stable within 2 hours
			String appTime = params.get("AppTime");
			int hhmm = Integer.parseInt(appTime);
			int h = (hhmm / 100) / 2 * 2;
			String timeBucket = String.format("%02d", h);
			return oKey + "|" + dKey + "|" + timeBucket + "|" + params.get("TransportCode");
		}

		private String bucketCoord(double v) {
			return String.format("%.3f", Math.round(v / 0.005) * 0.005);
		}

		@Override
		public Integer call() throws Exception {
			try {
			int heartbeatInterval = Integer.parseInt(System.getProperty("heartbeatInterval",
				prop.getProperty("heartbeatInterval", "500")));
			long taskStart = System.currentTimeMillis();
			for (Person p : listAgents) {
				int res = process(p);
				if (res < 0) {
					this.error++;
				}
				this.total++;
				if (heartbeatInterval > 0 && this.total % heartbeatInterval == 0) {
					Runtime rt = Runtime.getRuntime();
					long elapsed = (System.currentTimeMillis() - taskStart) / 1000;
					System.out.printf("  [task %d] %d/%d persons (%ds) [heap: %dMB / %dMB, cache: %d]%n",
						id, this.total, listAgents.size(), elapsed,
						(rt.totalMemory() - rt.freeMemory()) / (1024 * 1024),
						rt.maxMemory() / (1024 * 1024),
						routeCache.size());
				}
			}
			} catch (Throwable t) {
				System.err.println("[WebAPI TripTask " + id + "] failed: " + t);
				t.printStackTrace();
				if (t instanceof Exception) throw (Exception) t;
				throw new RuntimeException(t);
			}
			return 0;
		}
	}
	
	public void generate(List<Person> agents) {
		// prepare thread processing
		int availableCores = Runtime.getRuntime().availableProcessors();
		// Check JVM system property first (-DnumThreads=8), then config, then default
		String threadsProp = System.getProperty("numThreads",
			prop.getProperty("numThreads", String.valueOf(availableCores)));
		int numThreads = Integer.parseInt(threadsProp);
		System.out.println("NumOfThreads:" + numThreads + " (available cores: " + availableCores + ")");
		
		List<Callable<Integer> > listTasks = new ArrayList<>();
		int listSize = agents.size();
		int taskNum = numThreads;
		int stepSize = listSize / taskNum + (listSize % taskNum != 0 ? 1 : 0);
		for (int i = 0; i < listSize; i+= stepSize){
			int end = i + stepSize;
			end = Math.min(listSize, end);
			List<Person> subList = agents.subList(i, end);
			listTasks.add(new TripTask(i, subList));
		}
		System.out.println("NumOfTasks:" + listTasks.size());
		
		// execute thread processing
		ExecutorService es = Executors.newFixedThreadPool(numThreads);
		List<Future<Integer>> futures;
		try {
			futures = es.invokeAll(listTasks);
			es.shutdown();
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("WebAPI trip generation tasks interrupted", ex);
		}
		for (Future<Integer> f : futures) {
			try {
				f.get();
			} catch (ExecutionException ex) {
				throw new RuntimeException("WebAPI trip generation task failed", ex.getCause());
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new RuntimeException("WebAPI trip generation task interrupted", ex);
			}
		}
	}

	/**
	 * Call GetMixedRoute API.
	 *
	 * @return list of candidate JsonNodes on HTTP 200, or {@code null} on non-200
	 *         (signals session expiry / auth failure to the caller for retry).
	 */
	private static final AtomicInteger mixedRouteCallCount = new AtomicInteger();

	private static List<JsonNode> getMixedRoutes(CloseableHttpClient httpClient, String sessionid, String cookieHeader, Map<String, String> params) {
		String mixedRouteURL = prop.getProperty("api.getMixedRouteURL");
		if (mixedRouteURL == null) {
			throw new IllegalStateException("Missing config key: api.getMixedRouteURL");
		}
		// Log first call for diagnostics (session + URL verification)
		if (mixedRouteCallCount.incrementAndGet() == 1) {
			System.out.println("[API] First GetMixedRoute call → " + mixedRouteURL + " (session=" + sessionid + ")");
		}
		HttpPost mixedRoutePost = new HttpPost(mixedRouteURL);

		List<NameValuePair> mixedRouteParams = new ArrayList<>();
		for (Map.Entry<String, String> entry : params.entrySet()) {
			mixedRouteParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}

		try {
			mixedRoutePost.setEntity(new UrlEncodedFormEntity(mixedRouteParams));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		// Set all session cookies explicitly. The cookie jar alone is
		// unreliable when the relay is HTTP-only and the backend sets
		// Secure cookies — a compliant jar won't replay them over http://.
		mixedRoutePost.setHeader("Cookie", cookieHeader);

		HttpResponse mixedRouteResponse;
		try {
			mixedRouteResponse = executePostRequest(httpClient, mixedRoutePost);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		int status = mixedRouteResponse.getStatusLine().getStatusCode();
		if (status != 200) {
			System.err.println("[API] GetMixedRoute returned HTTP " + status + " (session=" + sessionid + ")");
			try { EntityUtils.consume(mixedRouteResponse.getEntity()); } catch (IOException ignored) {}
			return null; // non-200 → caller should attempt session refresh + retry
		}

		String body;
		try {
			body = EntityUtils.toString(mixedRouteResponse.getEntity());
		} catch (IOException e) {
			throw new RuntimeException("[API FAILURE] Failed to read GetMixedRoute response body", e);
		}
		if (body == null || body.trim().isEmpty()) {
			throw new RuntimeException("[API FAILURE] GetMixedRoute returned HTTP 200 but empty body");
		}

		// API returns NDJSON (application/x-ndjson): one JSON object per line, separated by \r\n
		List<JsonNode> candidates = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		int lineCount = 0;
		int parseFailCount = 0;
		for (String line : body.split("\r?\n")) {
			String trimmed = line.trim();
			if (trimmed.isEmpty()) continue;
			lineCount++;
			try {
				JsonNode node = mapper.readTree(trimmed);
				if (node.isObject()) {
					candidates.add(node);
				} else if (node.isInt()) {
					int code = node.asInt();
					if (code == 10001) {
						throw new RuntimeException("[API FAILURE] GetMixedRoute returned error 10001 "
							+ "(session not recognized by server). Session ID sent: " + sessionid
							+ "\nPossible causes:"
							+ "\n  1) Endpoint mismatch: CreateSession and GetMixedRoute hitting different servers"
							+ "\n     → Fix: set api.baseURL in config.local.properties"
							+ "\n  2) Proxy/relay not forwarding session state (if using localhost relay)"
							+ "\n     → Check proxy configuration maintains backend affinity"
							+ "\n  3) Session expired or was invalidated between creation and use"
							+ "\n  4) Load balancer routing to different backend instances"
							+ "\nCheck the [session] log lines above for the effective URLs and session ID.");
					}
				}
				// Other integer error codes (11000, 11024, etc.) are valid no-route responses
			} catch (IOException e) {
				parseFailCount++;
			}
		}
		if (lineCount > 0 && parseFailCount == lineCount) {
			throw new RuntimeException("[API FAILURE] GetMixedRoute returned HTTP 200 but all "
				+ lineCount + " lines failed JSON parse — response body may be HTML/error page");
		}
		return candidates;
	}

	private static JsonNode getRoadRoute(CloseableHttpClient httpClient, String sessionid, String cookieHeader, Map<String, String> params) throws Exception {
		String roadRouteURL = prop.getProperty("api.getRoadRouteURL");
		if (roadRouteURL == null) {
			throw new IllegalStateException("Missing config key: api.getRoadRouteURL");
		}
		HttpPost roadRoutePost = new HttpPost(roadRouteURL);

		List<NameValuePair> roadRouteParams = new ArrayList<>();
		for (Map.Entry<String, String> entry : params.entrySet()) {
			roadRouteParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}

		roadRoutePost.setEntity(new UrlEncodedFormEntity(roadRouteParams));
		roadRoutePost.setHeader("Cookie", cookieHeader);

		HttpResponse roadRouteResponse = executePostRequest(httpClient, roadRoutePost);
		ObjectMapper mapper = new ObjectMapper();

		if (roadRouteResponse.getStatusLine().getStatusCode() == 200) {
			String roadRouteResponseBody = EntityUtils.toString(roadRouteResponse.getEntity());
			JsonNode result = mapper.readTree(roadRouteResponseBody);
			if (result.isInt() && result.asInt() == 10001) {
				throw new RuntimeException("[API FAILURE] GetRoadRoute returned error 10001 "
					+ "(session not recognized by server). Session ID sent: " + sessionid
					+ " — see [session] log lines for diagnosis.");
			}
			return result;
		} else {
			System.out.println("Failed to get road route: " + roadRouteResponse.getStatusLine().getStatusCode());
			return mapper.readTree("");
		}
	}
	private static Properties prop;

	public static void main(String[] args) throws Exception {

		int start = 1;
		int end = 47;
		int mfactor = 1;
		if (args.length >= 1) {
			start = end = Integer.parseInt(args[0]);
		}
		if (args.length >= 2) {
			mfactor = Integer.parseInt(args[1]);
		}

		prop = ConfigLoader.load(start);

		String root = prop.getProperty("root");
		String inputDir = prop.getProperty("inputDir");
		System.out.println("Root Directory: " + root);
		System.out.println("Input Directory: " + inputDir);

		// api.baseURL: single override for the WebAPI server host.
		// When set, derives all three endpoint URLs automatically.
		String baseURL = prop.getProperty("api.baseURL");
		if (baseURL != null) {
			baseURL = baseURL.replaceAll("/+$", "");
			prop.setProperty("api.createSessionURL", baseURL + "/webapi/CreateSession");
			prop.setProperty("api.getRoadRouteURL",  baseURL + "/webapi/GetRoadRoute");
			prop.setProperty("api.getMixedRouteURL",  baseURL + "/webapi/GetMixedRoute");
			System.out.println("WebAPI base URL override: " + baseURL);
		}

		System.out.println("WebAPI endpoints:");
		System.out.println("  createSession = " + prop.getProperty("api.createSessionURL", "(NOT SET)"));
		System.out.println("  getMixedRoute = " + prop.getProperty("api.getMixedRouteURL", "(NOT SET)"));
		System.out.println("  getRoadRoute  = " + prop.getProperty("api.getRoadRouteURL", "(NOT SET)"));

		System.out.println("Memory/batch config:");
		System.out.println("  batchSize = " + System.getProperty("batchSize",
			prop.getProperty("batchSize", "10000")));
		System.out.println("  routeCache.maxEntries = " + System.getProperty("routeCache.maxEntries",
			prop.getProperty("routeCache.maxEntries", "10000"))
			+ " (0 = disabled)");
		Runtime rtStartup = Runtime.getRuntime();
		System.out.printf("  heap: %dMB used / %dMB max%n",
			(rtStartup.totalMemory() - rtStartup.freeMemory()) / (1024 * 1024),
			rtStartup.maxMemory() / (1024 * 1024));

		// Consistency check: all three URLs must use the same host
		try {
			String csHost = new java.net.URI(prop.getProperty("api.createSessionURL")).getHost();
			String mrHost = new java.net.URI(prop.getProperty("api.getMixedRouteURL")).getHost();
			String rrHost = new java.net.URI(prop.getProperty("api.getRoadRouteURL")).getHost();
			if (!csHost.equals(mrHost) || !csHost.equals(rrHost)) {
				System.err.println("WARNING: WebAPI endpoint hosts are INCONSISTENT:");
				System.err.println("  createSession → " + csHost);
				System.err.println("  getMixedRoute → " + mrHost);
				System.err.println("  getRoadRoute  → " + rrHost);
				System.err.println("  This WILL cause error 10001. Set api.baseURL in config.local.properties.");
			}
		} catch (Exception ignored) {
		}
		
		Country japan = new Country();

		// load data
		String railFile = String.format("%srailnetwork.tsv", inputDir+"/network/");
		Network railway = RailLoader.load(railFile);


		String cityFile = String.format("%scity_boundary.csv", inputDir);
		DataAccessor.loadCityData(cityFile, japan);

		String stationFile = String.format("%sbase_station.csv", inputDir);
		Network station = DataAccessor.loadLocationData(stationFile);
		japan.setStation(station);

		// Build combined transit stop network for reachability precheck (rail + bus)
		boolean precheckEnabled = Boolean.parseBoolean(prop.getProperty("api.precheckEnabled", "true"));
		Network transitStops = null;
		if (precheckEnabled) {
			transitStops = new Network(true, false);
			for (Node n : station.listNodes()) {
				transitStops.addNode(n);
			}
			System.out.println("[transitStops] Railway stations loaded: " + transitStops.nodeCount());

			String busStopDir = prop.getProperty("busStopDir", inputDir + "bus_stop");
			System.out.println("[transitStops] Bus stop directory: " + busStopDir);
			// Load bus stops for all target prefectures upfront (STRtree is immutable after first query)
			for (int i = start; i <= end; i++) {
				String busZipPath = BusStopLoader.getZipPath(busStopDir, i);
				if (new File(busZipPath).exists()) {
					Network busNet = BusStopLoader.load(busZipPath);
					for (Node n : busNet.listNodes()) {
						transitStops.addNode(n);
					}
				} else {
					System.err.println("[transitStops] Bus stop file not found: " + busZipPath);
				}
			}
			System.out.println("[transitStops] Total transit stops (rail + bus): " + transitStops.nodeCount());
		} else {
			System.out.println("[transitStops] Precheck disabled (api.precheckEnabled=false)");
		}

		String outputDir = prop.getProperty("outputDir", root);

		// Load per-city parameter group mapping (if available and enabled).
		// Disabled during tuning runs (paramGroup.enabled=false) because the
		// param-group files are what tuning is trying to CREATE — fail-fast
		// on missing files would kill the tuning run before it starts.
		boolean paramGroupEnabled = Boolean.parseBoolean(
			prop.getProperty("paramGroup.enabled", "true"));
		ParamGroupLoader paramGroups = null;
		if (paramGroupEnabled) {
			String projectRoot = System.getProperty("user.dir", ".");
			String paramGroupMappingPath = prop.getProperty("paramGroup.mappingCsv",
				projectRoot + "/data/tuning/city_code_to_param_group.csv");
			String paramGroupDir = prop.getProperty("paramGroup.dir",
				projectRoot + "/config/tuning/param_groups/");
			File mappingFile = new File(paramGroupMappingPath);
			File pgDir = new File(paramGroupDir);
			if (mappingFile.exists() && pgDir.isDirectory()) {
				paramGroups = ParamGroupLoader.load(
					mappingFile.getAbsolutePath(), pgDir.getAbsolutePath());
			} else {
				System.out.println("[paramGroup] No param group mapping found at "
					+ mappingFile.getAbsolutePath() + " — using base config for all cities");
			}
		} else {
			System.out.println("[paramGroup] Disabled (paramGroup.enabled=false) — using candidate config for all cities");
		}

		for (int i = start; i <= end; i++){

			File tripDir = new File(outputDir+"trip/", String.valueOf(i));
			File trajDir = new File(outputDir+"trajectory/", String.valueOf(i));
			System.out.println("Start prefecture:" + i +" "+ tripDir.mkdirs() +" "+ trajDir.mkdirs());

			String roadFile = String.format("%sdrm_%02d.tsv", inputDir+"/network/", i);

			Network road = DrmLoader.load(roadFile);
			String carKey = "car." + i;
			String carVal = prop.getProperty(carKey);
			if (carVal == null) {
				System.err.println("Missing config key: " + carKey + " -- skipping prefecture " + i);
				continue;
			}
			Double carRatio = Double.parseDouble(carVal);
			String bikeKey = "bike." + i;
			String bikeVal = prop.getProperty(bikeKey);
			if (bikeVal == null) {
				System.err.println("Missing config key: " + bikeKey + " -- defaulting bike ratio to 0.0 for prefecture " + i);
			}
			Double bikeRatio = bikeVal != null ? Double.parseDouble(bikeVal) : 0.0;

			// Try outputDir first (chained mainline), fall back to root (legacy/external activity data)
			// When chained, activity is already sampled — use mfactor=1 to avoid double-sampling
			File actDir = new File(outputDir + "activity/", String.valueOf(i));
			// tuning.loadScale: additional sampling on chained (pre-sampled) activity data.
			// Default 1 = load all (backward-compatible). Tuning sets higher values to cap sample size.
			int tuningLoadScale = Integer.parseInt(prop.getProperty("tuning.loadScale", "1"));
			int loadScale;
			if (!actDir.isDirectory()) {
				actDir = new File(root + "activity/", String.valueOf(i));
				loadScale = mfactor; // fallback: apply sampling here
				if (actDir.isDirectory()) {
					System.out.println("Activity input: " + actDir.getAbsolutePath() + " (fallback to root, mfactor=" + mfactor + ")");
				}
			} else {
				loadScale = tuningLoadScale; // chained: already base-sampled, apply tuning scale
				System.out.println("Activity input: " + actDir.getAbsolutePath() + " (chained, loadScale=" + loadScale + ")");
			}
			File[] actFiles = actDir.listFiles();
			if (actFiles == null) {
				System.err.println("Activity directory not found: " + actDir.getAbsolutePath());
				continue;
			}

			// Activity file order control (asc or desc by filename).
			// Used to split work between an original VM (asc) and a copied VM (desc).
			String activityFileOrder = System.getProperty("activityFileOrder",
				prop.getProperty("activityFileOrder", "asc")).trim().toLowerCase();
			java.util.Arrays.sort(actFiles, (a, b) -> a.getName().compareTo(b.getName()));
			if ("desc".equals(activityFileOrder)) {
				java.util.Collections.reverse(java.util.Arrays.asList(actFiles));
			}
			System.out.println("[activityFileOrder] order=" + activityFileOrder
				+ ", total files=" + actFiles.length);
			if (actFiles.length > 0) {
				System.out.println("[activityFileOrder] first: " + actFiles[0].getName());
				System.out.println("[activityFileOrder] last:  " + actFiles[actFiles.length - 1].getName());
			}

			// Group city files by param group so we create one worker per unique group
			Map<String, List<File>> filesByGroup = new LinkedHashMap<>();
			for (File file : actFiles) {
				if (!file.getName().contains(".csv")) continue;
				String baseName = file.getName().replace(".csv", "");
				String cityCode = baseName.substring("person_".length());
				int underscorePos = cityCode.indexOf('_');
				if (underscorePos > 0) {
					cityCode = cityCode.substring(0, underscorePos);
				}
				// Normalize city code to 5-digit zero-padded
				if (cityCode.matches("\\d+")) {
					cityCode = String.format("%05d", Integer.parseInt(cityCode));
				}
				String groupKey = "__base__"; // default: no param group overlay
				if (paramGroups != null) {
					String groupName = paramGroups.getGroupName(cityCode);
					if (groupName == null) {
						throw new RuntimeException("[paramGroup] FAIL-FAST: No param group mapping for city "
							+ cityCode + ". Add this city to city_code_to_param_group.csv before running.");
					}
					// Verify the .properties file exists for the mapped group
					if (paramGroups.getParamsForCity(cityCode) == null) {
						throw new RuntimeException("[paramGroup] FAIL-FAST: City " + cityCode
							+ " maps to group '" + groupName + "' but " + groupName
							+ ".properties is missing in config/tuning/param_groups/. "
							+ "Run tuning or copy the file from the owning VM before retrying.");
					}
					groupKey = groupName;
				}
				filesByGroup.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(file);
			}

			// Process each param group: create worker with overlaid params, then process cities
			for (Map.Entry<String, List<File>> entry : filesByGroup.entrySet()) {
				String groupKey = entry.getKey();
				List<File> groupFiles = entry.getValue();

				// Apply param group overlay to create effective properties
				Properties effectiveProp;
				if ("__base__".equals(groupKey)) {
					effectiveProp = prop;
				} else {
					effectiveProp = paramGroups.overlayForCity(prop,
						// use first file's city code to look up the group
						extractCityCode(groupFiles.get(0).getName()));
					System.out.println("[paramGroup] Group " + groupKey
						+ " (" + groupFiles.size() + " files): "
						+ "fare.per.hour=" + effectiveProp.getProperty("fare.per.hour", "?")
						+ " fatigue.walk=" + effectiveProp.getProperty("fatigue.walk", "?")
						+ " fare.init=" + effectiveProp.getProperty("fare.init", "?")
						+ " transferPenalty=" + effectiveProp.getProperty("api.transit.transferPenalty", "?"));
				}

				// Create worker with effective params (one session per param group)
				Properties savedProp = prop;
				prop = effectiveProp;
				TripGenerator_WebAPI_refactor worker = new TripGenerator_WebAPI_refactor(japan, road, railway, transitStops);
				prop = savedProp;

				int cityCount = 0;
				int batchSize = Integer.parseInt(System.getProperty("batchSize",
					prop.getProperty("batchSize", "10000")));

				// Resume support: check for force flag
				boolean forceTrip = Boolean.parseBoolean(
					System.getProperty("forceTrip",
						prop.getProperty("forceTrip", "false")));

				for (File file : groupFiles) {
					if (!file.getName().contains(".csv")) continue;
					String cityCode = extractCityCode(file.getName());
					String tripFileName = outputDir + "trip/" + i + "/trip_" + cityCode + ".csv";
					String trajectoryFileName = outputDir + "trajectory/" + i + "/trajectory_" + cityCode + ".csv";
					String doneMarker = outputDir + "trip/" + i + "/done_" + cityCode;

					// Resume: skip cities with a done marker (both files fully written)
					if (!forceTrip && new File(doneMarker).exists()) {
						System.out.println("[resume] Skipping completed city " + cityCode
							+ " (done marker exists)");
						cityCount++;
						continue;
					}
					// Partial output: rerun from scratch (first batch uses append=false)
					if (!forceTrip && (new File(tripFileName).exists() || new File(trajectoryFileName).exists())) {
						System.out.println("[resume] Reprocessing partial city " + cityCode
							+ " (output exists but no done marker)");
					}

					long starttime = System.currentTimeMillis();
					List<Person> agents = PersonAccessor.loadActivity(file.getAbsolutePath(), loadScale, carRatio, bikeRatio);
					int totalPersons = agents.size();
					System.out.printf("[city %s, group %s] %s (%d persons, batchSize=%d)%n",
						cityCode, groupKey, file.getName(), totalPersons, batchSize);

					// Process in batches to limit within-city memory:
					// generate() adds trips + trajectory to each person (~50KB/person),
					// so 800K persons × 50KB = 40GB if held in memory simultaneously.
					// Batching keeps peak memory at batchSize × 50KB + cache.
					int processed = 0;
					while (processed < totalPersons) {
						int batchEnd = Math.min(processed + batchSize, totalPersons);
						List<Person> batch = agents.subList(processed, batchEnd);

						worker.generate(batch);

						// Append results to output files (first batch creates, rest append)
						boolean append = (processed > 0);
						PersonAccessor.writeTrips(tripFileName, batch, append);
						PersonAccessor.writeTrajectory(trajectoryFileName, batch, append);
						if (worker.costLogEnabled) {
							worker.flushCostLog(outputDir + "costlog/" + i + "/costlog_" + cityCode + ".csv", append);
						}

						// Release trip + trajectory data immediately after writing
						for (Person p : batch) {
							p.listTrips().clear();
							p.clearTrajectory();
						}

						processed = batchEnd;
						if (totalPersons > batchSize) {
							Runtime rt = Runtime.getRuntime();
							System.out.printf("  [batch %d/%d persons] [heap: %dMB used / %dMB max, cache: %d]%n",
								processed, totalPersons,
								(rt.totalMemory() - rt.freeMemory()) / (1024 * 1024),
								rt.maxMemory() / (1024 * 1024),
								worker.routeCache.size());
						}
					}

					// Clear activity data (only needed during generate, not for output)
					for (Person p : agents) {
						p.clearActivity();
					}
					agents.clear();

					// Write done marker — signals that both trip and trajectory are complete
					try {
						new File(doneMarker).createNewFile();
					} catch (IOException e) {
						System.err.println("[resume] Warning: could not write done marker: " + doneMarker);
					}

					cityCount++;
					long endtime = System.currentTimeMillis();
					Runtime rt = Runtime.getRuntime();
					System.out.printf("%s: %dms [heap: %dMB used / %dMB max, cache: %d entries, cities: %d]%n",
						file.getName(), (endtime - starttime),
						(rt.totalMemory() - rt.freeMemory()) / (1024 * 1024),
						rt.maxMemory() / (1024 * 1024),
						worker.routeCache.size(), cityCount);
				}

				// Print mixed-route diagnostics for this group
				printDiagnostics(worker, i, groupKey);

				// Release route cache after each param group to prevent
				// unbounded growth across cities (dominant OOM source)
				System.out.println("[memory] Clearing route cache (" + worker.routeCache.size() + " entries)");
				worker.routeCache.clear();
			}

		}
		System.out.println("end");
	}

	/** Extract city code from filename: "person_22101.csv" -> "22101", "person_22101_labor.csv" -> "22101" */
	private static String extractCityCode(String fileName) {
		String baseName = fileName.replace(".csv", "");
		String cityCode = baseName.substring("person_".length());
		int underscorePos = cityCode.indexOf('_');
		if (underscorePos > 0) {
			cityCode = cityCode.substring(0, underscorePos);
		}
		return cityCode;
	}

	/** Mode order for tour decisions (same order as the legacy cost map, so ties resolve the same way). */
	private static final ETransport[] TOUR_MODES = {ETransport.CAR, ETransport.WALK, ETransport.BICYCLE, ETransport.MIX};

	/** One trip of a person's day: the cost of every mode first, the decided mode afterwards. */
	private static final class Leg {
		final Activity pre;
		final Activity next;
		final int tripId;
		double distance;
		Route route;
		Map<ETransport, Double> costs = new LinkedHashMap<>();
		final JsonNode[] transit = new JsonNode[1];
		ETransport baseline = ETransport.NOT_DEFINED;  // cheapest per trip without extra cost
		ETransport mode = ETransport.NOT_DEFINED;      // final decision
		boolean origTarget;
		boolean destTarget;
		String tourType = "";

		Leg(Activity pre, Activity next, int tripId) {
			this.pre = pre;
			this.next = next;
			this.tripId = tripId;
		}
	}

	private static String fmtCost(Double v) {
		return v == null ? "" : String.format(Locale.ROOT, "%.1f", v);
	}

	/** Write and clear the cost-log rows collected during generate() (debug.costLog=true). */
	public void flushCostLog(String fileName, boolean append) throws IOException {
		File f = new File(fileName);
		if (f.getParentFile() != null) f.getParentFile().mkdirs();
		try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, append), "UTF-8"))) {
			if (!append) {
				bw.write("person_id,trip_id,orig_gcode,dest_gcode,purpose,distance_m,car_avail,"
						+ "cost_car,cost_walk,cost_bicycle,cost_mix,mode_baseline,mode_final,tour_type");
				bw.newLine();
			}
			String row;
			while ((row = costLogRows.poll()) != null) {
				bw.write(row);
				bw.newLine();
			}
		}
	}

	private static void printDiagnostics(TripGenerator_WebAPI_refactor worker, int pref, String groupKey) {
		System.out.println("--- Mixed-route diagnostics (pref " + pref + ", group " + groupKey + ") ---");
		System.out.println("  AppDate: " + worker.appDate + ", TransportCode: " + worker.transportCode + ", selection: " + worker.transitSelection);
		System.out.println("  Trips below distance threshold (<" + worker.MIN_TRANSIT_DISTANCE + "m): " + worker.mixedBelowDistThreshold.get());
		System.out.println("  Mixed-route API queries: " + worker.mixedQueryCount.get());
		System.out.println("  Transit available (best candidate found): " + worker.mixedTransitAvailable.get());
		System.out.println("  No valid candidate: " + worker.mixedNoStationCount.get());
		System.out.println("  Candidates evaluated: " + worker.mixedCandidateTotal.get()
			+ " (bus-only: " + worker.mixedCandidateBusOnly.get()
			+ ", train-only: " + worker.mixedCandidateTrainOnly.get()
			+ ", mixed: " + worker.mixedCandidateMixed.get() + ")");
		System.out.println("  MIX mode selected (won cost comparison): " + worker.mixedSelectedCount.get()
			+ " (selected bus-only: " + worker.selectedBusOnly.get()
			+ ", train-only: " + worker.selectedTrainOnly.get()
			+ ", mixed: " + worker.selectedMixed.get() + ")");
		int totalEligible = worker.precheckSkipCount.get() + worker.precheckPassCount.get() + worker.mixedQueryCount.get();
		System.out.println("  --- Precheck ---");
		System.out.println("  Precheck skipped (no transit stop near OD): " + worker.precheckSkipCount.get());
		System.out.println("  Precheck passed: " + worker.precheckPassCount.get());
		if (totalEligible > 0) {
			System.out.printf("  Precheck reduction: %.1f%% of eligible trips%n",
				100.0 * worker.precheckSkipCount.get() / totalEligible);
		}
		System.out.println("  --- Cache ---");
		System.out.println("  Cache hits: " + worker.cacheHitCount.get());
		System.out.println("  Cache misses (actual API calls): " + worker.cacheMissCount.get());
		System.out.println("  Cache entries: " + worker.routeCache.size()
			+ " (max: " + worker.routeCacheMaxEntries + ", evictions: " + worker.cacheEvictCount.get() + ")");
		if (worker.cacheHitCount.get() + worker.cacheMissCount.get() > 0) {
			System.out.printf("  Cache hit rate: %.1f%%%n",
				100.0 * worker.cacheHitCount.get() / (worker.cacheHitCount.get() + worker.cacheMissCount.get()));
		}
		System.out.printf("  Avg API call time: %.0f ms%n",
			worker.cacheMissCount.get() > 0 ? (double) worker.cacheSavedMs.get() / worker.cacheMissCount.get() : 0);
		System.out.println("  --- Session ---");
		System.out.println("  Session refreshes: " + worker.sessionRefreshCount.get());
		System.out.println("  Retry after refresh succeeded: " + worker.sessionRetrySuccessCount.get());
		System.out.println("  Retry after refresh failed: " + worker.sessionRetryFailCount.get());
		long sessionAge = (System.currentTimeMillis() - worker.sessionCreatedAt) / 1000;
		System.out.println("  Final session age: " + sessionAge + "s");
		System.out.println("  --- Tour / extra car cost ---");
		System.out.println("  tour.enabled=" + worker.tourEnabled
			+ ", car.availability.perPerson=" + worker.carAvailPerPerson
			+ ", targetPrefs=" + worker.extraCostPrefs
			+ ", parking=" + worker.extraParking
			+ ", delayPerTrip=" + worker.extraDelayPerTrip
			+ ", costLog=" + worker.costLogEnabled);
		System.out.println("  Simple round-trip tours decided jointly: " + worker.tourSimpleCount.get()
			+ ", other tours (trip by trip, car consistency): " + worker.tourComplexCount.get());
		System.out.println("  Trips whose final mode differs from the per-trip cheapest without extra cost: "
			+ worker.modeChangedCount.get());
		System.out.println("---");
	}
}
