import type { NextPage } from 'next';
import Head from 'next/head';
import MetroStatus from '../components/MetroStatus';

const Home: NextPage = () => {
  return (
    <div>
      <Head>
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <main className="p-4">
        <MetroStatus />
      </main>
    </div>
  );
};

export default Home;
