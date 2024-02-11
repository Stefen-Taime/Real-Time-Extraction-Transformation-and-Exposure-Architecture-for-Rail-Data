import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Typography from '@mui/material/Typography';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import TrainIcon from '@mui/icons-material/Train';
import { createTheme, ThemeProvider } from '@mui/material/styles';

const theme = createTheme({
  palette: {
    primary: { main: '#556cd6' },
    secondary: { main: '#19857b' },
    error: { main: '#d32f2f' },
    success: { main: '#4caf50' },
    background: { default: '#f0f2f5' },
  },
});

type MetroStatusData = {
  identifier: string;
  latest_delay_minutes: number;
  direction: string;
  passage_time: number;
  passage_timeid: string;
  station_name: string;
  latitude: number;
  longitude: number;
};

const MetroStatus: React.FC = () => {
  const [status, setStatus] = useState<MetroStatusData[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const result = await axios('http://localhost:8001/data/');
        setStatus(result.data.data);
      } catch (error) {
        setError("Could not fetch metro status data");
        console.error(error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
    const intervalId = setInterval(fetchData, 5000);

    return () => clearInterval(intervalId);
  }, []);

  const formatPassageTime = (passageTimeId: string) => {
    const match = passageTimeId.match(/(\d{2})(\d{2})$/);
    if (match) {
      return `${match[1]}:${match[2]}`;
    }
    return "Unknown time";
  };

  const filterTrains = (trains: MetroStatusData[]) => {
    const now = new Date();
    return trains.filter((train) => {
      const match = train.passage_timeid.match(/(\d{2})(\d{2})$/);
      if (!match) return false;

      const passageTime = new Date();
      passageTime.setHours(parseInt(match[1], 10), parseInt(match[2], 10), 0, 0);
      passageTime.setMinutes(passageTime.getMinutes() + train.latest_delay_minutes);

      return passageTime > now;
    });
  };

  const filteredStatus = filterTrains(status);

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <ThemeProvider theme={theme}>
      <div style={{ margin: "20px" }}>
        {filteredStatus.length > 0 ? (
          filteredStatus.map((item) => (
            <Card key={item.identifier} style={{ marginBottom: "20px", backgroundColor: theme.palette.background.default }}>
              <CardContent>
                <Typography variant="h5" component="div" color="primary">
                  <TrainIcon /> {item.station_name}
                </Typography>
                <Typography color="text.secondary">
                  <AccessTimeIcon color="secondary" /> {item.direction} - {formatPassageTime(item.passage_timeid)}
                </Typography>
                {item.latest_delay_minutes !== 0 ? (
                  <Typography variant="body2" color="error">
                    Delay: {Math.abs(item.latest_delay_minutes)} min
                  </Typography>
                ) : (
                  <Typography variant="body2" color="success">
                    On time
                  </Typography>
                )}
              </CardContent>
            </Card>
          ))
        ) : (
          <Typography>No trains are currently scheduled.</Typography>
        )}
      </div>
    </ThemeProvider>
  );
};

export default MetroStatus;
