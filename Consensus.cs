// Copyright 2017 Bobby Powers. All rights reserved.
// Use of this source code is governed by the ISC
// license that can be found in the LICENSE file.

namespace Raft
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class VoteLedger
    {
        readonly int PeerCount;
        readonly int QuorumSize;

        public IList<PeerId> Votes { get; set; } = new List<PeerId>();
        // explicit responses we've received where we didn't get the
        // vote. |Votes| + |Nacks| == TotalReplyCount
        public IList<PeerId> Nacks { get; set; } = new List<PeerId>();

        private bool _completed;
        private CancellationToken _token;
        private TaskCompletionSource<bool> _completionSource;

        internal VoteLedger(Config config, CancellationToken token)
        {
            _token = token;
            _completionSource = new TaskCompletionSource<bool>();

            _token.Register(() => {
                    _completed = true;
                    _completionSource.TrySetCanceled();
                });
            QuorumSize = config.Peers.Count / 2 + 1;
            PeerCount = config.Peers.Count;
        }

        internal void Record(RequestVoteResponse response)
        {
            if (_completed)
                return;

            if (response.VoteGranted)
            {
                Votes.Add(response.Sender);
            }
            else
            {
                Nacks.Add(response.Sender);
            }


            if (Votes.Count >= QuorumSize || Nacks.Count >= QuorumSize ||
                Votes.Count + Nacks.Count == PeerCount)
            {
                _completed = true;
                _completionSource.TrySetResult(Votes.Count >= QuorumSize);
            }
        }

        internal Task<bool> WaitMajority(IEnumerable<Task<IPeerResponse>> responses, CancellationToken cancellationToken)
        {
            foreach (var responseTask in responses)
            {
                Task.Run(async () => {
                        try
                        {
                            var result = await responseTask;
                            if (result is RequestVoteResponse response)
                            {
                                this.Record(response);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"WaitMajority exception: {e}");
                        }
                    }, _token);
            }

            return _completionSource.Task;
        }

        bool Elected()
        {
            return Votes.Count >= QuorumSize;
        }

        bool VoteFailed()
        {
            return Nacks.Count >= QuorumSize;
        }
    }

    internal sealed class Consensus<TWriteOp>
    {
        public enum State
        {
            Disconnected = 1 << 0, 
            Candidate    = 1 << 1,
            Leader       = 1 << 2,
            Follower     = 1 << 3,
        }

        private Term _currentTerm = new Term(0);
        private PeerId? _votedFor = null;
        private ILog<TWriteOp> _log;
        private Log log = new Log();

        // Volatile state
        private LogIndex _commitIndex = new LogIndex(0);
        private LogIndex _lastApplied = new LogIndex(0);

        // Leader state
        private Dictionary<PeerId, int> _nextIndex;
        private Dictionary<PeerId, int> _matchIndex;

        private Config _config;
        public State _state = State.Disconnected;
        internal PeerRpcDelegate _peerRpc;

        internal PeerId Id { get; private set; }

        private Random _random;
        private CancellationTokenSource _timeoutCancellationSource;
        private DateTime _lastHeartbeat;

        private CancellationTokenSource _electionCancellationSource;

        internal Consensus(PeerId id, Config config, ILog<TWriteOp> log)
        {
            _config = config;
            Id = id;
            _random = new Random(_config.PrngSeed[id]);
            _peerRpc = _config.PeerRpcDelegate;
            if (_peerRpc == null)
                throw new InvalidOperationException("PerformPeerRpc must be set in Config");
            _log = log;
        }

        internal async Task<IPeerResponse> HandleVoteRequest(RequestVoteRequest request)
        {
            bool voteGranted = false;
            var lastTerm = Term.Invalid;
            if (log.Length > 0)
                lastTerm = log.Get(_lastApplied).Term;
            //verifica daca cel pe care il voteaza are term mai mare sau egal, daca ultimul log ii mai mare sau egal ca al lui 
            //
                Console.WriteLine(
                    $" de la nod{Id.N} cu termn {_currentTerm.N} cu last index {_lastApplied.N} {lastTerm.N}vote pt {request.CandidateId.N} cu term{request.Term.N} cu lastIndex{request.LastLogIndex.N} cu term del alast log {request.LastLogTerm.N}");
                if (request.Term >= _currentTerm &&
                    (request.LastLogIndex >= _lastApplied && request.LastLogTerm >= lastTerm))
                {
                // (!_votedFor.HasValue || _votedFor.Value == request.CandidateId) deleted
                    

                    if (_state != State.Follower)
                    {
                    //
                        ResetElectionTimer();
                        await TransitionToFollower();
                    }
                    _currentTerm = request.Term;
                    Console.WriteLine("vote");
                    voteGranted = true;
                    _votedFor = request.CandidateId;
                    ResetElectionTimer();
                }

            return new RequestVoteResponse()
            {
                Sender = Id,
                Term = _currentTerm,
                VoteGranted = voteGranted,
            };
        }

        internal async Task<IPeerResponse> HandleAppendEntriesRequest(AppendEntriesRequest<TWriteOp> entry)
        {
            var ceva = entry.Term;
            var  r = entry.PrevLogIndex;
            var rr = entry.Entries;
            var t = entry.LeaderId;
            var s = entry.PrevLogTerm;

            if (log.LogEntries.Count < entry.Entries.Count)
            {
                log.LogEntries = entry.Entries;
                Console.WriteLine($" append new log {log.LogEntries.Last()?.Operation}");
            }
            return new RequestVoteResponse()
            {
                Sender = Id,
                Term = _currentTerm,
                VoteGranted = false,
            };
        }


        internal Task<IPeerResponse> HandlePeerRpc(IPeerRequest request)
        {
            switch (request) {
                case RequestVoteRequest voteRequest:
                    return HandleVoteRequest(voteRequest);
                case AppendEntriesRequest<TWriteOp> appendEntriesRequest:
                    return HandleAppendEntriesRequest(appendEntriesRequest);
                default:
                    Console.WriteLine($"{Id.N} Got UNHANDLED PeerRpc request {request}");
                    break;
            }

            return Task.FromResult((IPeerResponse)null);
        }

        private Task TransitionToDisconnected()
        {
            _state = State.Disconnected;
            

            if (_electionCancellationSource != null)
            {
                _electionCancellationSource.Cancel();
                _electionCancellationSource.Dispose();
                _electionCancellationSource = null;
            }
            return Task.CompletedTask;
        }

        private async Task TransitionToLeader()
        {
            // only a candidate can be a leader

            if (_state != State.Candidate)
            {
                await TransitionToCandidate();
            }

            // cancel any previous election we are a candidate in (e.g. it timed out)
            if (_electionCancellationSource != null)
            {
                _electionCancellationSource.Cancel();
                _electionCancellationSource.Dispose();
                _electionCancellationSource = null;
            }

            Console.WriteLine($"Look at me, I ({Id.N})  with state {_state} am the leader now of term {_currentTerm.N}.");
            Console.WriteLine($"Do you want to cancel node {Id.N}? y/n");
            var answer = Console.ReadLine();
            if (answer == "y")
            {
                //_currentTerm.N --; 
                Console.WriteLine($"lenght {log.Length} ");
                _currentTerm.N = -1; 
                //TransitionToFollower();
                await TransitionToDisconnected();
                Console.WriteLine("deleted");
                return;
            }
            
            _lastHeartbeat = DateTime.Now;

            Console.WriteLine("call heartbeat");
                // send now, and TODO: repeat during idle periods to prevent election timeout
                await Heartbeat();
                _state = State.Leader;
        }

        private void HandleClientRequest()
        {
            // If command received from client: append entry to local log,
            // respond after entry applied to state machine (§5.3)

            // If last log index ≥ nextIndex for a follower: send
            // AppendEntries RPC with log entries starting at nextIndex

        }

        private Task Heartbeat()
        {
            // send AppendEntries RPCs to each server
            // if that server's nextIndex <= log index, send w/ log entries starting at nextIndex
            // else send empty AppendEntries RPC

            var lastIndex = log.LogEntries.Count;
            var index = new LogIndex(lastIndex++);
            var entry = new LogEntry()
            {
                Index = index,
                Operation = $"heartbeat from {Id.N}",
                Term = _currentTerm
            };
            log.LogEntries.Add(entry);
            Console.WriteLine("heartbeat");

            IEnumerable<Task<IPeerResponse>> responses =
                _config.Peers.Where(id => id.N != Id.N).Select(id => _peerRpc(id, new AppendEntriesRequest<TWriteOp>()
                    {
                        Term = _currentTerm,
                        LeaderId = Id,
                        PrevLogIndex = _lastApplied,
                        PrevLogTerm = _currentTerm,
                        Entries = log.LogEntries,
                        LeaderCommit = _lastApplied,
                    }));


            bool receivedMajority = false;
             
            // await responses
            return Task.CompletedTask;
        }

        private void SendAppendEntriesRpc()
        {

        }

        private async Task TransitionToCandidate()
        {
            // Leaders and disconnected servers cannot transition
            // directly to candidates.

            if (_state == State.Leader || _state == State.Disconnected)
            {
                await TransitionToFollower();
            }
 
            // ensure we are now in Candidate state
            _state = State.Candidate;
            Console.WriteLine($"candidate {Id.N}");

            // cancel any previous election we are a candidate in (e.g. it timed out)
            if (_electionCancellationSource != null)
            {
                _electionCancellationSource.Cancel();
                _electionCancellationSource.Dispose();
                _electionCancellationSource = null;
            }

            _electionCancellationSource = new CancellationTokenSource();
            var cancellationToken = _electionCancellationSource.Token;

            _currentTerm.N++;

            var ledger = new VoteLedger(_config, cancellationToken);
            // vote for ourselves
            ledger.Record(new RequestVoteResponse()
                {
                    Sender = Id,
                    Term = _currentTerm,
                    VoteGranted = true,
                });
            Console.WriteLine($"vote for {Id.N}, term {_currentTerm.N}");
            // record that this round, we are voting for ourselves
            _votedFor = Id;

            // we reset the election timer right before
            // TransitionToCandidate is called

            var lastTerm = Term.Invalid;
            // FIXME: should _lastApplied be -1, and check for that here?
            if (log.Length > 0)
                lastTerm = log.Get(_lastApplied).Term;

            IEnumerable<Task<IPeerResponse>> responses =
                _config.Peers.Where(id => id.N != Id.N).Select(id => _peerRpc(id, new RequestVoteRequest()
                    {
                        Term = _currentTerm,
                        CandidateId = Id,
                        LastLogIndex = _lastApplied,
                        LastLogTerm = lastTerm,
                    }));

            bool receivedMajority = false;
            try
            {
                receivedMajority = await ledger.WaitMajority(responses, cancellationToken);
            }
            catch (TaskCanceledException)
            {
                return;
            }

            if (!receivedMajority)
            {
                _electionCancellationSource.Cancel();
                _electionCancellationSource.Dispose();
                _electionCancellationSource = null;
                Console.WriteLine($"Node({Id.N}) {_state} {_currentTerm.N}: received {ledger.Votes.Count}/{_config.Peers.Count} ({ledger.Votes.Count + ledger.Nacks.Count} responses)");
                return;
            }

            Console.WriteLine($"Node({Id.N}): received {ledger.Votes.Count}/{_config.Peers.Count} ({ledger.Votes.Count + ledger.Nacks.Count} responses)");

            await TransitionToLeader();
        }

        // TODO: this isn't an _election_ timeout, it is a timeout
        // that results in an election
        private TimeSpan RandomElectionTimeout()
        {
            var timeoutSpan = (int)_config.ElectionTimeoutSpan.TotalMilliseconds;
            var randomWait = Time.Milliseconds(_random.Next(timeoutSpan));
            return _config.ElectionTimeoutMin.Add(randomWait);
        }

        private async Task ElectionTimeoutTask(CancellationToken cancelationToken)
        {
            var timeout = RandomElectionTimeout();
            Console.WriteLine($" timeout{timeout} node {Id.N} temen{_currentTerm.N}");

            // loop, sleeping for ~ the broadcast (timeout) time.  If
            // we have gone too long without
            while (!_timeoutCancellationSource.IsCancellationRequested)
            {
                var sinceHeartbeat = DateTime.Now - _lastHeartbeat;
                if (sinceHeartbeat > timeout)
                {
                    ResetElectionTimer();
                    Supervised.Run(function: TransitionToCandidate);
                    continue;
                }

                try
                {
                    var sinceHeartbeat2 = DateTime.Now - _lastHeartbeat;
                    await Task.Delay(timeout - sinceHeartbeat2, cancelationToken);
                }
                catch (TaskCanceledException)
                {
                    // cancelled or disposed means we are no longer a
                    // follower, so end this task
                    return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
            }
        }

        private Task TransitionToFollower()
        {
            if (_state != State.Follower )
            {
                _state = State.Follower;
                _timeoutCancellationSource = new CancellationTokenSource();

                Supervised.Run(() => ElectionTimeoutTask(_timeoutCancellationSource.Token));
            }
            
            return Task.CompletedTask;
        }

        private void ResetElectionTimer()
        {
            _lastHeartbeat = DateTime.Now;
        }

        // Initialize this node, which means transitioning from
        // Disconnected -> Candidate -> (Leader || Follower)
        internal async Task Init()
        {
            ResetElectionTimer();

            await TransitionToFollower();
        }
    }
}
